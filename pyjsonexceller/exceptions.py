class FunctionNotFoundError(Exception):
    pass


class PluginError(Exception):
    pass


class PluginDefinitionError(PluginError):
    pass


class PluginNotFoundError(PluginError):
    pass
