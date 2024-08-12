from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class Annotations(object):

  class Enum(object):
    CONFIG_ROW_KEY = PropertiesFromEnumValue('', 'config_row', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CONFIG_ROW_SCHEMA_KEY = PropertiesFromEnumValue('', 'config_row_schema', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SCHEMATRANSFORM_URN_KEY = PropertiesFromEnumValue('', 'schematransform_urn', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MANAGED_UNDERLYING_TRANSFORM_URN_KEY = PropertiesFromEnumValue('', 'managed_underlying_transform_urn', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class ExpansionMethods(object):

  class Enum(object):
    JAVA_CLASS_LOOKUP = PropertiesFromEnumValue('beam:expansion:payload:java_class_lookup:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SCHEMA_TRANSFORM = PropertiesFromEnumValue('beam:expansion:payload:schematransform:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

