import { awscdk, javascript } from 'projen';
const project = new awscdk.AwsCdkTypeScriptApp({
  projenrcTs: true,
  cdkVersion: '2.204.0',
  defaultReleaseBranch: 'master',
  name: 'cdk-renovo-rl-quicksight',
  deps: [
    'cdk-nag',
    '@aws-cdk/aws-glue-alpha',
  ],
  githubOptions: {
    pullRequestLintOptions: {
      semanticTitle: false,
    },
  },
  depsUpgrade: true,
  depsUpgradeOptions: {
    workflow: false,
    exclude: ['projen'],
  },
  gitignore: [
    '.venv',
    '**/__pycache__',
  ],
});

new javascript.UpgradeDependencies(project, {
  include: ['projen'],
  taskName: 'upgrade-projen',
  workflow: false,
});

project.synth();