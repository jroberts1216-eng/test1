import { IAspect, ITaggable, TagManager } from 'aws-cdk-lib';
import { IConstruct } from 'constructs';

//Define the tag type
type Tags = { [key: string]: string } & {
  'map-migrated': string;
};

//implement the IAspect interface which provides the visit method, an implementation of the Visitor Pattern,
//it visits every contruct in the app tree

export class ApplyTags implements IAspect {
  #tags: Tags;

  constructor(tags: Tags) {
    this.#tags = tags;
  }

  //visit each node and check to see if the resource is taggable, if so apply the tags

  visit(node: IConstruct) {
    if (TagManager.isTaggable(node)) {
      Object.entries(this.#tags).forEach(([key, value]) => {
        this.applyTag(node, key, value);
      });
    }
  }

  applyTag(resource: ITaggable, key: string, value: string) {
    resource.tags.setTag(key, value);
  }
}
