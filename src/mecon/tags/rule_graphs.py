import logging
from collections.abc import Iterable
from itertools import chain
from typing import Literal

import networkx as nx
import pandas as pd

from mecon.tags import tagging, tag_helpers


def _to_tag_names(tags: Iterable[tagging.Tag | str] | tagging.Tag | str) -> list[str] | str:
    if isinstance(tags, str):
        return tags
    if isinstance(tags, tagging.Tag):
        return tags.name

    tag_names = [tag.name if isinstance(tag, tagging.Tag) else tag for tag in tags]
    return tag_names


class TagGraph:
    def __init__(self, tags: Iterable[tagging.Tag], dependency_mapping: dict):
        self._tags = tags
        self._quick_lookup = {tag.name: tag for tag in self._tags}
        self._dependency_mapping = dependency_mapping

        self._tidy_table_cache: pd.DataFrame | None = None

    @property
    def tags(self):
        return self._tags

    def _as_tag(self, tag: tagging.Tag | str) -> tagging.Tag:
        return tag if isinstance(tag, tagging.Tag) else self._quick_lookup[tag]

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
        df_tags_l0['depends_on'] = [[]] * len(df_tags_l0)

        df_mapping = pd.concat([df_tags_agg, df_tags_l0]).set_index('tag').to_dict('index')

        return df_mapping

    def create_networkx_graph(self):
        df = self.tidy_table()
        # Create a directed graph
        G = nx.DiGraph()

        # Add edges based on the DataFrame
        for _, row in df.iterrows():
            tag = row['tag']
            depends_on = row['depends_on']
            if pd.notna(depends_on):
                G.add_edge(depends_on, tag)

        return G

    def find_all_cycles(self):
        G = self.create_networkx_graph()

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

        new_df = pd.DataFrame(cleaned_edges, columns=['tag', 'depends_on']).groupby('tag').agg(
            {'depends_on': list}).reset_index()
        new_df['depends_on'] = new_df['depends_on'].apply(
            lambda arr: [dep for dep in arr if pd.notna(dep) and str(dep).strip() != '']
        )
        new_dep_mapping = new_df.set_index('tag').to_dict('index')

        new_tg = AcyclicTagGraph(self._tags, new_dep_mapping)
        logging.info(
            f"Removed cycles from the graph. {cycles=}, {edges_to_remove=}, {len(edges)=}, {len(cleaned_edges)=}, {len(new_tg.find_all_cycles())=}")
        logging.info(f"{[edge in cleaned_edges for edge in edges_to_remove]=}")
        return new_tg

    # def create_plotly_graph(self, k=.5, levels_col=None):
    #     from mecon.data.graphs import create_plotly_graph
    #     df = self.tidy_table()
    #     return create_plotly_graph(df, from_col='tag', to_col='depends_on', k=k, levels_col=levels_col)

    def select_subgraph_df(self, tags: list[tagging.Tag | str]) -> pd.DataFrame:
        subgraph_tag_names = [tag.name if isinstance(tag, tagging.Tag) else tag for tag in tags]
        depended_cond = self.tidy_table()['depends_on'].isin(subgraph_tag_names)
        dependee_cond = self.tidy_table()['tag'].isin(subgraph_tag_names)
        select_condition = dependee_cond | depended_cond
        subgraph_df = self.tidy_table()[select_condition]
        return subgraph_df

class AcyclicTagGraph(TagGraph):
    def __init__(self,
                 tags: Iterable[tagging.Tag],
                 dependency_mapping: dict,
                 if_has_cycles: Literal['raise', 'remove'] = 'remove', ):
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
            raise ValueError(
                f"Cannot calculate hierarchy on a graph with cycles: {self.has_cycles()=}, {self.find_all_cycles()=}")

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

            tag_level = max(dep_levels) + 1
            mapping[tag]['level'] = tag_level
            return tag_level

        for tag, info in mapping.items():
            _calc_level_rec(tag)

        self._dependency_mapping = mapping
        # The mapping gained a 'level' column; any previously cached tidy
        # table is now stale.
        self._tidy_table_cache = None

    def all_tag_dependencies(self, tag: tagging.Tag | str) -> list[tagging.Tag] | None: # TODO add _rec to the name since it's recursive
        tag_name = _to_tag_names(tag)

        if tag_name not in self._dependency_mapping:
            return None
        direct_deps_names = self._dependency_mapping[tag_name]['depends_on']
        direct_deps_as_tags = [self._quick_lookup[dep_name] for dep_name in direct_deps_names if dep_name in self._quick_lookup]

        if len(direct_deps_as_tags) == 0:
            return []

        _rec_results = [self.all_tag_dependencies(dep_tag) for dep_tag in direct_deps_as_tags]
        rec_deps_flat = list(chain(*[deps for deps in _rec_results if deps is not None]))

        res = list(set(rec_deps_flat + direct_deps_as_tags))
        return res

    def get_immediate_parent_tags(self, tag: tagging.Tag | str) -> list[tagging.Tag]:
        tag = self._as_tag(tag)
        if isinstance(tag, tagging.Tag):
            tag = tag.name

        parent_tags_str = self.tidy_table()[self.tidy_table()['depends_on'] == tag]['tag'].unique().tolist()
        parent_tags = set([self._quick_lookup[tag] for tag in parent_tags_str])
        return list(parent_tags)

    def all_parent_tags_rec(self, tag: tagging.Tag | str) -> list[tagging.Tag]:
        tag = self._as_tag(tag)
        immediate_parent_tags = self.get_immediate_parent_tags(tag)
        all_parent_tags = set(immediate_parent_tags)
        for p_tag in immediate_parent_tags:
            imm_parents = self.all_parent_tags_rec(p_tag)
            if len(imm_parents) == 0:
                continue
            all_parent_tags = all_parent_tags.union(imm_parents)

        return list(all_parent_tags)

    def all_tags_affected_by(self, tag: tagging.Tag | str) -> list[tagging.Tag]:
        tag = self._as_tag(tag)
        res = self.all_parent_tags_rec(tag) + [tag] + self.all_tag_dependencies(tag)
        return res

    def subgraph_containing_tag(self, tag: tagging.Tag | str) -> list[tagging.Tag]:
        tag = self._as_tag(tag)
        to_be_checked = set(self.all_tags_affected_by(tag))
        have_been_checked = set()
        while len(to_be_checked) > 0:
            target_tag = to_be_checked.pop()
            affected_tags = set(self.all_tags_affected_by(target_tag))
            have_been_checked = have_been_checked.union({target_tag})
            to_be_checked = to_be_checked.union(affected_tags.difference(have_been_checked))

        return list(have_been_checked)

    def find_all_root_tags(self) -> list[tagging.Tag]:
        root_tags = []
        for tag in self._tags:
            parent_tags = self.get_immediate_parent_tags(tag)
            if len(parent_tags) > 0:
                continue
            root_tags.append(tag)
        return root_tags


    # def find_all_tag_subgraphs(self) -> list[list[tagging.Tag]]:
    #     G = self.create_networkx_graph()
    #     UG = G.to_undirected()
    #
    #     subgraphs = list(UG.subgraph(c).copy() for c in nx.connected_components(UG))
    #     subgraph_lists = [list(sg.nodes) for sg in subgraphs]
    #     # subgraph_lists = [list([self._as_tag(n) for n in sg.nodes]) for sg in subgraphs]
    #     pass


    # def get_subgraph_df(self, tag: tagging.Tag | str) -> pd.DataFrame:
    #     subgraph_tags = self.all_tag_dependencies(tag)
    #     subgraph_tag_names = [tag.name for tag in subgraph_tags]
    #     subgraph_df = self._tidy_table_cache[
    #         self._tidy_table_cache['tag'].isin(subgraph_tag_names) | self._tidy_table_cache['depends_on'].isin(
    #             subgraph_tag_names)]
    #     return subgraph_df

