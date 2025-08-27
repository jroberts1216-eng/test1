import {
  DatabaseDetails,
  TableDetails,
  RenovoLiveDBSchemas,
} from '../../src/common';
import {
  DmsDataTypes,
  DmsSelectionRuleActions,
  DmsTransformationRuleActions,
  DmsTransformationRuleRuleTargets,
  ingestionTransformationRules,
} from '../../src/dms';

/**
 * The RenovoMaster database details
 */
export const dbRenovoMaster: DatabaseDetails = {
  databaseName: 'RenovoMaster',
  tables: [
    {
      tableName: 'l_lookups',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'l_lookupKey',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'l_mfgModalities',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'l_mfgModalityID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'l_pmSchedules',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'l_pmSchedID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'l_seTimeTypesExt',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'l_lookupKey',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'localPermissions',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'localPermissionID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'mtDeviceAcqPricing',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'id',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'mtDevices',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'mtDeviceID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'mtFrequencyChangeQueue',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'id',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'mtModels',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'mtModelID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'mtVendors',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'mtVendorID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'organizations',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'orgID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'SageRegions',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'ID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'userOrgs',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'ID',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
    {
      tableName: 'users',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'userid',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ],
      transformationRules: [
        {
          ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
            columnName: 'user_passwordHash',
          },
          ruleAction: DmsTransformationRuleActions.REMOVE_COLUMN,
        },
      ]
    },
    {
      tableName: 'l_setimetypesext',
      schema: RenovoLiveDBSchemas.DBO,
      primaryKey: 'l_lookupKey',
      selectionRules: [
        {
          objectLocator: {
            schemaName: RenovoLiveDBSchemas.DBO,
          },
          ruleAction: DmsSelectionRuleActions.INCLUDE,
        }
      ]
    },
  ],
  selectionRules: [],
  transformationRules: ingestionTransformationRules,
};

/**
 * The Renovo client database details
 */
export const renovoClientTables: TableDetails[] = [
  {
    tableName: 'clients',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'clientID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'clientSystems',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'systemID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'coverage_coverageDetailsTemplates',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'coverage_CoverageDetailsTemplateId',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'coverage_coverageSpecification',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'coverage_CoverageSpecId',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'documents',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'documentID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'facilities',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'facilityID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'facility_costCenters',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'facility_costCenterID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'facility_equipmentBudget',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'FacilityEquipmentBudgetId',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'facility_equipment',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'facility_equipmentID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'ismSystems',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'ismSystemID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_equipmentStatus',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'l_equipmentStatusID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_seDiagnosis',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'seDiagnID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_seSpecialConditions',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'seSpecCondID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_seStatus',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'seStatusID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_seResolutions',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'seResID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_seSymptoms',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'seSymptID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'locationRole_permissions',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'localPermissionID, locationRoleID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'locationRole_users',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'locationKey, locationRoleID, userID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'locationRoles',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'locationRoleID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'mtclAEM',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'mtclAEMID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'mtclDevices',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'mtclDeviceID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'mtclDeviceTemps',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'mtclDeviceTempID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
    transformationRules: [
      {
        ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsTransformationRuleActions.ADD_COLUMN,
        expression: "'TEMP: ' ||  UPPER($mtclDevTmp_vendor) ||  ' - ' ||  $mtclDevTmp_model ||  ', ' ||  $mtclDevTmp_device ",
        dataType: {
          type: DmsDataTypes.STRING,
          length: 100,
          scale: '',
        },
        value: 'mtclDevTmp_formattedName',
      },
    ]
  },
  {
    tableName: 'mtclModels',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'mtclModelID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'po_lineItems',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'poLineItemID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'po_purchaseOrders',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'purchaseOrderID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'po_vendorPayments',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'poVendorPaymentID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'po_vpLineItems',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'poVpLineItemID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'po_liExchanges',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'poLineItemID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_assignedUsers',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'se_assignedUserID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_partsUsed',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'se_partsUsedID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_serviceEvents',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'serviceEventID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_serviceNotes',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'se_serviceNoteID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_specialConditions',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'l_seSpecialConditionID, se_serviceEventID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_time',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'se_timeID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'vendors2',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'vendorId',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'vendorInvoices',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'vendorInvoiceId',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'vs_vendorSubContract',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'vendorSubContractID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'vsc_lineItems',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'vscLineItemID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'regions',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'regionID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'clUser_roles',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'roleID, userID',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'clRole_permissions',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'id',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'clientcontracts',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'clientcontractid',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'billingruns',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'billingrunid',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'se_time',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'se_timeid',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'billingrunaudit',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'billingrunauditid',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'l_partcategories',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'l_partcategoryid',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'billingrunadjustments',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'billingrunadjustmentid',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  },
  {
    tableName: 'servicebillinglaborrates',
    schema: RenovoLiveDBSchemas.DBO,
    primaryKey: 'id',
    selectionRules: [
      {
        objectLocator: {
          schemaName: RenovoLiveDBSchemas.DBO,
        },
        ruleAction: DmsSelectionRuleActions.INCLUDE,
      },
    ],
  }
];

export const renovoClientDbTransformations = ingestionTransformationRules.concat([
  {
    ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
    objectLocator: {
      schemaName: RenovoLiveDBSchemas.ANY,
      tableName: '%',
      columnName: '%',
      dataType: DmsDataTypes.UINT1,
    },
    ruleAction: DmsTransformationRuleActions.CHANGE_DATA_TYPE,
    dataType: {
      type: DmsDataTypes.INT4,
    }
  },
  {
    ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
    objectLocator: {
      schemaName: RenovoLiveDBSchemas.ANY,
      tableName: '%',
      columnName: '%',
      dataType: DmsDataTypes.UINT8,
    },
    ruleAction: DmsTransformationRuleActions.CHANGE_DATA_TYPE,
    dataType: {
      type: DmsDataTypes.INT8,
    }
  },
  {
    ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
    objectLocator: {
      schemaName: RenovoLiveDBSchemas.ANY,
      tableName: '%',
      columnName: '%',
      dataType: DmsDataTypes.UINT4,
    },
    ruleAction: DmsTransformationRuleActions.CHANGE_DATA_TYPE,
    dataType: {
      type: DmsDataTypes.INT4,
    }
  },
]);

export const dbClRenovo: DatabaseDetails = {
  databaseName: 'clRenovo',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClDev: DatabaseDetails = {
  databaseName: 'clDev',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClCape: DatabaseDetails = {
  databaseName: 'clCape',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClCarle: DatabaseDetails = {
  databaseName: 'clCarle',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClCentro: DatabaseDetails = {
  databaseName: 'clCentro',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClChrist: DatabaseDetails = {
  databaseName: 'clChrist',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClCvp: DatabaseDetails = {
  databaseName: 'clCvp',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClDemo: DatabaseDetails = {
  databaseName: 'clDemo',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClEcmc: DatabaseDetails = {
  databaseName: 'clEcmc',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClMmc: DatabaseDetails = {
  databaseName: 'clMmc',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClMms: DatabaseDetails = {
  databaseName: 'clMms',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClRadon: DatabaseDetails = {
  databaseName: 'clRadon',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClRadonSc: DatabaseDetails = {
  databaseName: 'clRadonSc',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClRadonVa: DatabaseDetails = {
  databaseName: 'clRadonVa',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClVertexUk: DatabaseDetails = {
  databaseName: 'clVertexUk',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClWbs: DatabaseDetails = {
  databaseName: 'clWbs',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

export const dbClZoli: DatabaseDetails = {
  databaseName: 'clZoli',
  tables: renovoClientTables,
  transformationRules: renovoClientDbTransformations,
};

