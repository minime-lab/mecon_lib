

class Singleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def set_instance(cls, instance):
        cls._instance = instance

    @classmethod
    def reset_instance(cls):
        cls._instance = None


class InstanceAlreadyExistError(Exception):
    pass


class InstanceDoesNotExistError(Exception):
    pass


class Multiton:
    """
    Multiton is a generalised version of a Singleton, that extends to multiple instances
    that cannot be duplicated. Each instance will be identified and accessed by the
    instance_name, almost identical to a Flyweight.

    Any subclass of the Multiton class will automatically make the subclass a Multiton.
    The hierarchical tree beneath the first subclass of the Multiton will belong to
    the same multiton situation.
    """
    def __init__(self, instance_name):
        if instance_name in self._get_instances():
            raise InstanceAlreadyExistError
        self._instance_name = instance_name
        self._get_instances()[self._instance_name] = self

    @classmethod
    def _get_instances(cls):
        if not hasattr(cls, '_instances'):
            cls._instances = {}
        return cls._instances

    @property
    def instance_name(self):
        return self._instance_name

    @classmethod
    def from_key(cls, key):
        if key not in cls._get_instances():
            raise InstanceDoesNotExistError(f"Instance '{key=}' does not exist.")
        return cls._get_instances()[key]

    @classmethod
    def all_instances(cls):
        return cls._get_instances().values()

