from aggregation.ops.base import Aggregate
from aggregation.ops.operators import Reduce, Map, Filter
from mql.mql_value import of
from pprint import pprint
from json import dumps


def add_field_output():
    """Initial Paradigm leveraging builder syntax and enforcing types at the method level"""
    pipeline = (
        Aggregate()
        .add_fields(core=of(2), pour=of(3), pike=of(4))
        .group(_id={"x": "$x"}, y=of({"$sum": "$y"}))
        .project()
    )
    print(dumps(pipeline.as_pyobj(), indent=4))


def design_doc_output_equiv():
    """
    db.test.aggregate(
        [
            {
                "$project": {
                    "result": {
                        "$reduce": {
                            "input": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$numList",
                                            "cond": {
                                                "$eq": [{"$mod": ["$$this", 2]}, 0]
                                            },
                                        }
                                    },
                                    "in": {"$multiply": ["$$this", 10]},
                                }
                            },
                            "initialValue": 0,
                            "in": {"$add": ["$$value", "$$this"]},
                        }
                    }
                }
            }
        ]
    )
    """
    pipeline = Aggregate().project(
        result=Reduce(
            input=Map(
                input=Filter(input=of("$numList"), cond=of("$$this").mod(2).eq(0)),
                _in=of("$$this").multiply(10)
            ),
            initialValue=of(0),
            _in=of("$$value").add("$$this")
        )
    )
    pprint(pipeline.as_pyobj())


def design_doc_output():
    """
    This example does not use the MqlDocument as the base.
    A helper function needs to be made to convert a Pydantic Object --> MqlDocument
    or a Document --> a Pydantic object.

    Thinking about providing an "assign_schema" option that can get passed between the two values.
    Not Implemented:
    example_collection.match(lambda d: d._id.eq("A")))
        .addField(
            lambda d: d.result,
            lambda d: (
                d.numList.
                .filter(lambda n: n.mod(2).eq(0))
                .map(lambda n: n.multiply(10))
                .reduce(0, lambda a, b: b.add(a))
            )
        )
    In provided example, a,b, and d will all be MqlDocument.

    Assign_schema would involve taking values from pydantic-typed library:
    - odmantic, beanie
    or a language with known models, and porting them to the MqlDocument syntax.

    This is just to create the appropriate mutation operators.
    i.e.
    mongoengine -> MqlDocument -> aggregate().get_results() -> mongoengine
    """
    pass


design_doc_output_equiv()