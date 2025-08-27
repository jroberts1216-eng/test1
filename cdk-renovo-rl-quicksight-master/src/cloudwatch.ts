import {
  aws_cloudwatch as cloudwatch,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

/**
 * Properties for QuickSight CloudWatch Dashboard
 */
export interface QuickSightCloudWatchDashboardProps extends StackProps {
  /**
   * QuickSight Dashboard ID to monitor
   */
  quickSightDashboardId: string;

  /**
   * QuickSight Dataset ID for ingestion metrics
   */
  datasetId: string;

  /**
   * CloudWatch Dashboard Name
   */
  dashboardName: string;
}

/**
 * Stack that creates a CloudWatch Dashboard for QuickSight metrics
 */
export class QuickSightCloudWatchDashboard extends Stack {
  constructor(scope: Construct, id: string, props: QuickSightCloudWatchDashboardProps) {
    super(scope, id, props);

    // Create CloudWatch Dashboard
    const dashboard = new cloudwatch.Dashboard(this, 'QuickSightMetricsDashboard', {
      dashboardName: props.dashboardName,
    });

    const dashboardId = props.quickSightDashboardId;
    const datasetId = props.datasetId;

    // SPICE Capacity Metrics (Aggregate)
    const spiceCapacityLimitWidget = new cloudwatch.GraphWidget({
      title: 'SPICE Capacity Limit',
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'SPICECapacityLimitInMB',
          statistic: 'Average',
          unit: cloudwatch.Unit.MEGABYTES,
        }),
      ],
      width: 8,
    });

    const spiceCapacityUsedWidget = new cloudwatch.GraphWidget({
      title: 'SPICE Capacity Used',
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'SPICECapacityConsumedInMB',
          statistic: 'Average',
          unit: cloudwatch.Unit.MEGABYTES,
        }),
      ],
      width: 8,
    });

    const spiceCapacityUtilizationWidget = new cloudwatch.GraphWidget({
      title: 'SPICE Capacity Utilization (%)',
      left: [
        new cloudwatch.MathExpression({
          expression: '(consumed / limit) * 100',
          usingMetrics: {
            consumed: new cloudwatch.Metric({
              namespace: 'AWS/QuickSight',
              metricName: 'SPICECapacityConsumedInMB',
              statistic: 'Average',
            }),
            limit: new cloudwatch.Metric({
              namespace: 'AWS/QuickSight',
              metricName: 'SPICECapacityLimitInMB',
              statistic: 'Average',
            }),
          },
          label: 'SPICE Utilization (%)',
        }),
      ],
      width: 8,
    });

    // Data Ingestion Metrics
    const ingestionInvocationCountWidget = new cloudwatch.GraphWidget({
      title: `Ingestion Invocation Count - Dataset ${datasetId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'IngestionInvocationCount',
          dimensionsMap: { DatasetId: datasetId },
          statistic: 'Sum',
        }),
      ],
      width: 12,
    });

    const ingestionRowCountWidget = new cloudwatch.GraphWidget({
      title: `Ingestion Row Count - Dataset ${datasetId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'IngestionRowCount',
          dimensionsMap: { DatasetId: datasetId },
          statistic: 'Sum',
        }),
      ],
      width: 12,
    });

    const ingestionErrorCountWidget = new cloudwatch.GraphWidget({
      title: `Ingestion Error Count - Dataset ${datasetId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'IngestionErrorCount',
          dimensionsMap: { DatasetId: datasetId },
          statistic: 'Sum',
        }),
      ],
      width: 12,
    });

    const ingestionLatencyWidget = new cloudwatch.GraphWidget({
      title: `Ingestion Latency - Dataset ${datasetId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'IngestionLatency',
          dimensionsMap: { DatasetId: datasetId },
          statistic: 'Average',
          unit: cloudwatch.Unit.MILLISECONDS,
        }),
      ],
      width: 12,
    });

    // Performance and Availability Metrics
    const dashboardViewLoadTimeWidget = new cloudwatch.GraphWidget({
      title: `Dashboard View Load Time - Dashboard ${dashboardId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'DashboardViewLoadTime',
          dimensionsMap: { DashboardId: dashboardId },
          statistic: 'Average',
          unit: cloudwatch.Unit.MILLISECONDS,
        }),
      ],
      width: 12,
    });

    const visualLoadTimeWidget = new cloudwatch.GraphWidget({
      title: `Visual Load Time - Dashboard ${dashboardId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'VisualLoadTime',
          dimensionsMap: { DashboardId: dashboardId },
          statistic: 'Average',
          unit: cloudwatch.Unit.MILLISECONDS,
        }),
      ],
      width: 12,
    });

    const dashboardViewCountWidget = new cloudwatch.GraphWidget({
      title: `Dashboard View Count - Dashboard ${dashboardId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'DashboardViewCount',
          dimensionsMap: { DashboardId: dashboardId },
          statistic: 'Sum',
          unit: cloudwatch.Unit.COUNT,
        }),
      ],
      width: 12,
    });

    const visualLoadErrorCountWidget = new cloudwatch.GraphWidget({
      title: `Visual Load Error Count - Dashboard ${dashboardId}`,
      left: [
        new cloudwatch.Metric({
          namespace: 'AWS/QuickSight',
          metricName: 'VisualLoadErrorCount',
          dimensionsMap: { DashboardId: dashboardId },
          statistic: 'Sum',
          unit: cloudwatch.Unit.COUNT,
        }),
      ],
      width: 12,
    });

    // Create section headers with dashboard and dataset IDs
    const performanceHeader = new cloudwatch.TextWidget({
      markdown: `### QuickSight Dashboard Performance Metrics\n**Dashboard ID: ${dashboardId}**`,
      width: 24,
      height: 2,
    });

    const ingestionHeader = new cloudwatch.TextWidget({
      markdown: `### QuickSight Data Ingestion Metrics\n**Dataset ID: ${datasetId}**`,
      width: 24,
      height: 2,
    });

    const capacityHeader = new cloudwatch.TextWidget({
      markdown: '### QuickSight SPICE Capacity Metrics\n**Account Level Metrics**',
      width: 24,
      height: 2,
    });

    // Add all widgets to the dashboard in the new order with section headers
    dashboard.addWidgets(
      // Performance metrics section (dashboard related)
      performanceHeader,
      dashboardViewLoadTimeWidget,
      visualLoadTimeWidget,
      dashboardViewCountWidget,
      visualLoadErrorCountWidget,

      // Ingestion metrics section (dataset related)
      ingestionHeader,
      ingestionInvocationCountWidget,
      ingestionRowCountWidget,
      ingestionErrorCountWidget,
      ingestionLatencyWidget,

      // SPICE capacity metrics section (aggregate)
      capacityHeader,
      spiceCapacityLimitWidget,
      spiceCapacityUsedWidget,
      spiceCapacityUtilizationWidget,
    );
  }
}
