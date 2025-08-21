import aws_cdk as core
import aws_cdk.assertions as assertions

from dubai_dataset.dubai_dataset_stack import DubaiDatasetStack

# example tests. To run these tests, uncomment this file along with the example
# resource in dubai_dataset/dubai_dataset_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DubaiDatasetStack(app, "dubai-dataset")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
