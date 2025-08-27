import {
  aws_quicksight as quicksight,
  cloudformation_include as cfninc,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { RenovoLiveEnv } from '../common';
import * as path from 'path';

export interface RenovoLiveDmDataSetProps extends StackProps {
  /**
   * The data source to be used for this dataset
   */
  readonly dataSource: quicksight.CfnDataSource;
  /**
   * The RenovoLive env this dataset is for
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The RenovoLive database this dataset is for
   */
  readonly database: string;
  /**
   * The ARN of the folder to place the dataset in
   */
  readonly folderArn: string;
  /**
   * Whether to create refresh schedules for this dataset. Must be false on the first creation
   * 
   * @default false
   */
  readonly createRefreshSchedules?: boolean;
  /**
   * Row level permissions data set arn
   */
  readonly rowLevelPermissionsDataSetArn: string;
}

export class RenovoLiveDmDataSet extends Stack {
  constructor(scope: Construct, id: string, props: RenovoLiveDmDataSetProps) {
    super(scope, id, props);

    new cfninc.CfnInclude(this, 'vQsDmTemplate', {
      templateFile: path.join(__dirname, 'dm.json'),
      parameters: {
        DataSourceArn: props.dataSource.attrArn,
        Database: props.database,
        FolderArn: props.folderArn,
        RenovoLiveEnv: props.renovoLiveEnv,
        ManagingPrincipal: `arn:aws:quicksight:${Stack.of(this).region}:${Stack.of(this).account}:group/default/RenovoLiveDevelopers`,
        RowLevelPermissionsDataSetArn: props.rowLevelPermissionsDataSetArn
      }
    });

    if (props.createRefreshSchedules) {
      new quicksight.CfnRefreshSchedule(this, 'vQsDmRefreshSchedule', {
        dataSetId: `${props.renovoLiveEnv}_${props.database}_v_qs_dm`,
        awsAccountId: Stack.of(this).account,
        schedule: {
          scheduleId: '1',
          refreshType: 'INCREMENTAL_REFRESH',
          scheduleFrequency: {
            interval: 'MINUTE15',
          }
        }
      });
  
      new quicksight.CfnRefreshSchedule(this, 'vQsDmFullRefreshSchedule', {
        dataSetId: `${props.renovoLiveEnv}_${props.database}_v_qs_dm`,
        awsAccountId: Stack.of(this).account,
        schedule: {
          scheduleId: '2',
          refreshType: 'FULL_REFRESH',
          scheduleFrequency: {
            interval: 'DAILY',
          }
        }
      });      
    }
  }
}