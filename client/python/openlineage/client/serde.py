# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import sys
import logging
from enum import Enum
from typing import List, Dict

import attr

log = logging.getLogger(__name__)

try:
    import numpy
except ImportError:
    log.warning("ImportError occurred when trying to import numpy module.")


class Serde:
    @classmethod
    def remove_nulls_and_enums(cls, obj):
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, Dict):
            return dict(filter(
                lambda x: x[1] is not None,
                {k: cls.remove_nulls_and_enums(v) for k, v in obj.items()}.items()
            ))
        if isinstance(obj, List):
            return list(filter(lambda x: x is not None and (isinstance(x, dict) and x != {}), [
                cls.remove_nulls_and_enums(v) for v in obj if v is not None
            ]))

        # Pandas can use numpy.int64 object
        if 'numpy' in sys.modules and isinstance(obj, numpy.int64):
            return int(obj)
        return obj

    @classmethod
    def to_dict(cls, obj):
        if not isinstance(obj, dict):
            obj = attr.asdict(obj)
        return cls.remove_nulls_and_enums(obj)

    @classmethod
    def to_json(cls, obj):
        return json.dumps(
            cls.to_dict(obj),
            sort_keys=True,
            default=lambda o: f"<<non-serializable: {type(o).__qualname__}>>"
        )

    @classmethod
    def to_atlan_database(cls, obj):
        return json.dumps(
            {
                "entities": [
                    {
                        "typeName": "Database",
                        "attributes": {
                            "name": obj.databaseName,
                            "qualifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}",
                            "connectorName": obj.connectorName,
                            "connectionQualifiedName": obj.connectionQualifiedName
                        }
                    }
                ]
            }
        )

    @classmethod
    def to_atlan_schema(cls, obj):
        return json.dumps(
            {
                "entities": [
                    {
                        "typeName": "Schema",
                        "attributes": {
                            "name": obj.schemaName,
                            "qualifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}/{obj.schemaName}",
                            "connectorName": obj.connectorName,
                            "databaseName": obj.databaseName,
                            "databaseQulifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}",
                            "connectionQualifiedName": obj.connectionQualifiedName
                        },
                        "relationshipAttributes": {
                            "database": {
                                "typeName": "Database",
                                "uniqueAttributes": {
                                    "qualifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}"
                                }
                            }
                        }
                    }
                ]
            }
        )

    @classmethod
    def to_atlan_table(cls, obj):
        def _create_table_dict(table_qualified_name):
            return {
                        "typeName": "Table",
                        "attributes": {
                            "name": table_qualified_name.split("/")[-1],
                            "qualifiedName": table_qualified_name,
                            "connectorName": obj.connectorName,
                            "schemaName": obj.schemaName,
                            "schemaQualifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}/{obj.schemaName}",
                            "databaseName": obj.databaseName,
                            "databaseQulifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}",
                            "connectionQualifiedName": obj.connectionQualifiedName
                        },
                        "relationshipAttributes": {
                            "atlanSchema": {
                                "typeName": "Schema",
                                "uniqueAttributes": {
                                    "qualifiedName": f"{obj.connectionQualifiedName}/{obj.databaseName}/{obj.schemaName}"
                                }
                            }
                        }
                    }

        tables = obj.inputTableQualifiedNames + obj.outputTableQualifiedNames
        return json.dumps(
            {
                "entities": [
                    _create_table_dict(table_qualified_name) for table_qualified_name in tables
                ]
            }
        )

    @classmethod
    def to_atlan_process(cls, obj):
        def _create_process_dict():
            return {
                "name": obj.name,
                "qualifiedName": obj.qualifiedName,
                "connectorName": obj.connectorName,
                "connectionName": obj.connectionName,
                "connectionQualifiedName": obj.connectionQualifiedName
            }

        def _create_table_dicts(table_name):
            return {
                "typeName": "Table",
                "uniqueAttributes": {
                    "qualifiedName": table_name
                }
            }

        return json.dumps(
            {
                "entities": [
                    {
                        "typeName": "Process",
                        "attributes": _create_process_dict(),
                        "relationshipAttributes": {
                            "inputs": [
                                _create_table_dicts(table_name) for table_name in obj.inputTableQualifiedNames
                            ],
                            "outputs": [
                                _create_table_dicts(table_name) for table_name in obj.outputTableQualifiedNames
                            ]
                        }
                    }
                ]
            }
        )

