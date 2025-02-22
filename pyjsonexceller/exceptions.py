class FunctionNotFound(Exception):
    pass


class PluginError(Exception):
    pass


class PluginDefinitionError(PluginError):
    pass


class PluginNotFound(PluginError):
    pass
