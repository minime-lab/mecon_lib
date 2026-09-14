import logging
from collections.abc import Iterable
from itertools import chain
from typing import Literal

import networkx as nx
import pandas as pd

from mecon.tags import tagging, tag_helpers


class TagGraph:
    def __init__(self, tags: Iterable[tagging.Tag], dependency_mapping: dict):
        self._tags = tags
        self._quick_lookup = {tag.name: tag for tag in self._tags}
        self._dependency_mapping = dependency_mapping

        self._tidy_table_cache: pd.DataFrame | None = None

    @property
    def tags(self):
        return self._tags

    def tidy_table(self, ignore_tags_with_no_dependencies: bool = False) -> pd.DataFrame:
        """Materialise the dependency mapping as a (long-format) DataFrame.

        Cached on first call. ``ignore_tags_with_no_dependencies`` is a pure
        OUTPUT filter — the cached full table is unchanged either way, so the
        filter does not invalidate anything.
        """
        if self._tidy_table_cache is None:
            tags = []
            for tag, info in self._dependency_mapping.items():
                info_cpy = info.copy()
                row_dict = {'tag': tag}
                depends_on = [dep for dep in info_cpy['depends_on'] if pd.notna(dep) and str(dep).strip() != '']
                del info_cpy['depends_on']
                row_dict.update(info_cpy)
                if len(depends_on) == 0:
                    row_dict['depends_on'] = None
                    tags.append(row_dict)
                else:
                    for dep_tag in depends_on:
                        tags.append(dict(**row_dict, depends_on=dep_tag))

            self._tidy_table_cache = pd.DataFrame(tags)

        if ignore_tags_with_no_dependencies:
            return self._tidy_table_cache.dropna(subset=['depends_on']).reset_index(drop=True)
        return self._tidy_table_cache

    @classmethod
    def from_tags(cls, tags: Iterable[tagging.Tag]) -> 'TagGraph':
        dependency_mapping = TagGraph.build_dependency_mapping(tags)
        return cls(tags, dependency_mapping)

    @classmethod
    def from_tags_dataframe(cls, tags_df: pd.DataFrame) -> 'TagGraph':
        """Build a TagGraph directly from a tags dataframe.

        Mirrors the way mecon_app's DataManager already materialises tags:
        the dataframe is expected to carry the tag-rule CSV schema
        (``name`` + ``conditions_json`` columns), each row is turned into a
        ``tagging.Tag`` via ``Tag.from_json_string``, and the work is
        delegated to ``from_tags``. Lets the graph be built from the same
        dataframe the app already loads, without first collecting Tag objects.
        """
        required = {'name', 'conditions_json'}
        if not required.issubset(tags_df.columns):
            missing = sorted(required.difference(tags_df.columns))
            raise ValueError(f"tags dataframe missing required columns: {missing}")

        tags = [
            tagging.Tag.from_json_string(row['name'], row['conditions_json'])
            for _, row in tags_df.iterrows()
        ]
        return cls.from_tags(tags)

    @staticmethod
    def build_dependency_mapping(tags: Iterable[tagging.Tag]) -> dict:
        def _normalise_dep_list(value) -> list[str]:
            if isinstance(value, list):
                raw = value
            elif value is None or pd.isna(value):
                return []
            else:
                raw = str(value).split(',')

            return [str(dep).strip() for dep in raw if pd.notna(dep) and str(dep).strip() != '']

        rules = {tag.name: tag_helpers.expand_rule_to_subrules(tag.rule) for tag in tags}
        expanded_rules = []
        for tag_name, tag_rules in rules.items():
            for rule in tag_rules:
                expanded_rules.append({'tag': tag_name, 'rule': rule})

        df = pd.DataFrame(expanded_rules)
        df['type'] = df['rule'].apply(lambda rule: type(rule).__name__)

        df_cnd = df[df['type'] == 'Condition'].copy()
        df_cnd['is_tag_rule'] = df_cnd['rule'].apply(lambda rule: rule.field == 'tags')

        df_tags = df_cnd[df_cnd['is_tag_rule']].copy()
        df_tags['depends_on'] = df_tags['rule'].apply(lambda rule: _normalise_dep_list(rule.value))

        df_tags_agg = df_tags.groupby('tag').agg({'depends_on': lambda arr: list(chain(*arr))}).reset_index()

        level_zero_tags = set(df['tag']).difference(df_tags_agg['tag'])
        df_tags_l0 = pd.DataFrame({'tag': list(level_zero_tags)})
        df_tags_l0['depends_on'] = [[]]*len(df_tags_l0)

        df_mapping = pd.concat([df_tags_agg, df_tags_l0]).set_index('tag').to_dict('index')

        return df_mapping

    def find_all_cycles(self):
        df = self.tidy_table()
        # Create a directed graph
        G = nx.DiGraph()

        # Add edges based on the DataFrame
        for _, row in df.iterrows():
            tag = row['tag']
            depends_on = row['depends_on']
            if pd.notna(depends_on):
                G.add_edge(depends_on, tag)

        # Find all simple cycles
        cycles = list(nx.simple_cycles(G))

        sizewise_sorted_cycles = sorted(cycles, key=len, reverse=True)
        return sizewise_sorted_cycles

    def has_cycles(self):
        return len(self.find_all_cycles()) > 0

    def remove_cycles(self) -> 'AcyclicTagGraph':
        edges = self.tidy_table()[['tag', 'depends_on']].values.tolist()
        cycles = self.find_all_cycles()

        edges_to_remove = []
        for cycle in cycles:
            cyclic_tags_ordered_based_on_insertion = [tag.name for tag in self._tags if tag.name in cycle]
            tag_link_to_remove = cyclic_tags_ordered_based_on_insertion[-1]
            edges_with_this_tag = [edge for edge in edges if edge[0] == tag_link_to_remove and edge[1] in cycle]
            edges_to_remove.extend(edges_with_this_tag)

        cleaned_edges = [edge for edge in edges if edge not in edges_to_remove]

        new_df = pd.DataFrame(cleaned_edges, columns=['tag', 'depends_on']).groupby('tag').agg({'depends_on': list}).reset_index()
        new_df['depends_on'] = new_df['depends_on'].apply(
            lambda arr: [dep for dep in arr if pd.notna(dep) and str(dep).strip() != '']
        )
        new_dep_mapping = new_df.set_index('tag').to_dict('index')

        new_tg = AcyclicTagGraph(self._tags, new_dep_mapping)
        logging.info(f"Removed cycles from the graph. {cycles=}, {edges_to_remove=}, {len(edges)=}, {len(cleaned_edges)=}, {len(new_tg.find_all_cycles())=}")
        logging.info(f"{[edge in cleaned_edges for edge in edges_to_remove]=}")
        return new_tg

    # def create_plotly_graph(self, k=.5, levels_col=None):
    #     from mecon.data.graphs import create_plotly_graph
    #     df = self.tidy_table()
    #     return create_plotly_graph(df, from_col='tag', to_col='depends_on', k=k, levels_col=levels_col)



class AcyclicTagGraph(TagGraph):
    def __init__(self,
                 tags: Iterable[tagging.Tag],
                 dependency_mapping: dict,
                 if_has_cycles: Literal['raise', 'remove'] = 'remove',):
        super().__init__(tags, dependency_mapping)

        if self.has_cycles():
            if if_has_cycles == 'raise':
                raise ValueError(f"Input tags contain cycles!")
            elif if_has_cycles == 'remove':
                logging.warning(f"WARNING: Input tags contain cycles! Removing cycles...")
                new_atg = self.remove_cycles()
                self._tags = new_atg._tags
                self._dependency_mapping = new_atg._dependency_mapping
                self._quick_lookup = new_atg._quick_lookup
                # Invalidate the tidy-table cache: it was populated from the
                # pre-cleanup mapping inside the has_cycles() call above and
                # still describes the cyclic graph.
                self._tidy_table_cache = None
            else:
                raise ValueError(f"Invalid if_has_cycles value: {if_has_cycles}!")

        # Always have hierarchy levels available; idempotent & cheap when already computed.
        self.add_hierarchy_levels()

    def levels(self) -> dict[str, int]:
        """Pure dict getter for the per-tag hierarchy level.

        Levels are computed once in ``__init__``, so this never mutates state.
        """
        return {tag: info['level'] for tag, info in self._dependency_mapping.items()}

    @classmethod
    def from_cyclic_tag_graph(cls, tag_graph: TagGraph) -> 'AcyclicTagGraph':
        return tag_graph.remove_cycles()

    def add_hierarchy_levels(self):
        if 'level' in self._dependency_mapping[list(self._dependency_mapping.keys())[0]]:
            return

        if self.has_cycles():
            raise ValueError(f"Cannot calculate hierarchy on a graph with cycles: {self.has_cycles()=}, {self.find_all_cycles()=}")

        mapping = self._dependency_mapping.copy()

        def _calc_level_rec(tag):
            if tag not in mapping:
                logging.warning(f"{tag} not in dependency mapping while calculating hierarchy. Will be replaced with 0")
                return 0

            if 'level' in mapping[tag]:
                return mapping[tag]['level']

            if len(mapping[tag]['depends_on']) == 0:
                mapping[tag]['level'] = 0
                return 0

            dep_levels = []
            for dep_tag in mapping[tag]['depends_on']:
                 dep_levels.append(_calc_level_rec(dep_tag))

            tag_level = max(dep_levels)+1
            mapping[tag]['level'] = tag_level
            return tag_level


        for tag, info in mapping.items():
            _calc_level_rec(tag)

        self._dependency_mapping = mapping
        # The mapping gained a 'level' column; any previously cached tidy
        # table is now stale.
        self._tidy_table_cache = None

    def find_all_root_tags(self) -> Iterable[tagging.Tag]:
        res = [tag for tag in self._tags if tag if len(self.tags_that_depends_on(tag))==0]
        return res

    def find_all_tag_subgraphs(self) -> Iterable[Iterable[tagging.Tag]]:
        subgraphs = {}
        for tag in self.find_all_root_tags():
            dependecies = self.all_tag_dependencies(tag)
            deps_id = f"{tag.name},"+','.join(sorted([tag.name for tag in dependecies if dependecies is not None]))
            subgraphs[deps_id] = [tag]+dependecies

        res = sorted(subgraphs.values(), key=len, reverse=True)
        return res


    def all_tag_dependencies(self, tag: tagging.Tag | str) -> Iterable[tagging.Tag] | None:
        tag_name = tag.name if isinstance(tag, tagging.Tag) else tag
        if tag_name not in self._dependency_mapping:
            return None
        direct_deps_names = self._dependency_mapping[tag_name]['depends_on']
        direct_deps = [self._quick_lookup[dep_name] for dep_name in direct_deps_names if dep_name in self._quick_lookup]

        if len(direct_deps) == 0:
            return []

        _rec_results = [self.all_tag_dependencies(dep_tag) for dep_tag in direct_deps]
        indirect_deps = list(chain(*[deps for deps in _rec_results if deps is not None]))
        res = direct_deps + indirect_deps
        return res

    def tags_that_depends_on(self, tag: tagging.Tag | str) -> Iterable[tagging.Tag] | None:
        tag_name = tag.name if isinstance(tag, tagging.Tag) else tag
        if tag_name not in self._dependency_mapping:
            return None

        direct_deps = [self._quick_lookup[curr_tag_name] for curr_tag_name, curr_tag_info in self._dependency_mapping.items() if tag_name in curr_tag_info['depends_on']]
        if len(direct_deps) == 0:
            return []

        _rec_results = [self.tags_that_depends_on(dep_tag) for dep_tag in direct_deps]
        indirect_deps = list(chain(*[deps for deps in _rec_results if deps is not None]))
        res = indirect_deps + direct_deps
        return res


    def all_tags_affected_by(self, tag: tagging.Tag | str) -> Iterable[tagging.Tag]:
        if isinstance(tag, str):
            tag = self._quick_lookup[tag]
        affected_root_tags = self.tags_that_depends_on(tag)
        if len(affected_root_tags) == 0:
            return set([tag]+self.all_tag_dependencies(tag))

        all_affected_tags_list = [[affected_root_tag]+self.all_tag_dependencies(affected_root_tag) for affected_root_tag in affected_root_tags]
        res = set(chain(*all_affected_tags_list))
        return res

    def get_subgraph_df(self, tag: tagging.Tag | str) -> Iterable[tagging.Tag]:
        subgraph_tags = self.all_tag_dependencies(tag)
        subgraph_tag_names = [tag.name for tag in subgraph_tags]
        subgraph_df = self._tidy_table_cache[self._tidy_table_cache['tag'].isin(subgraph_tag_names) | self._tidy_table_cache['depends_on'].isin(subgraph_tag_names)]
        return subgraph_df