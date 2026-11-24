# What is Strange Loop?

A workflow can give you an answer and still leave you wondering how it got there. Which step produced it? Which version of an input did that step see? Strange Loop records those relationships as the workflow runs. You can start at a result and follow its **lineage** back to the inputs that produced it.

## Pass a value in Python

```python
def double(number: int) -> int:
    return number * 2

def increment(number: int) -> int:
    return number + 1

def calculate(number: int) -> int:
    incr = increment(number)
    return double(incr)

answer = calculate(10)
print(answer)  # 22
```

The call to `increment()` produces the integer `11`, which `calculate()` passes to `double()` to get `22`. You can see how that happened by reading the code. The returned integer itself carries no record of the call.

## Run the same calculation with Strange Loop

Below is the same calculation implemented with Strange Loop. 

```python
from strange_loop.replay import Context, Loop, Version, stage

@stage
def double(number: int) -> int:
    return number * 2

@stage
def increment(number: int) -> int:
    return number + 1

async def calculate(ctx: Context, number: Version) -> Version:
    incr = await increment(number=number)
    dbl = await double(number=incr)
    return dbl

loop = Loop()
answer = await loop.run(calculate, number=10)
print(answer.value)  # 22
```

Three things have changed: 

1. The functions are decorated with `@stage` to indicate they are stages in the workflow.
2. The `calculate()` function is now asynchronous and uses `await` to call the stages. This is necessary to support asynchronous execution and proper tracking of inputs and outputs within the loop. The stages themselves can be synchronous or asynchronous functions.
3. The execution of the workflow is managed by a `Loop` instance, and its `run` method returns a [version](./versions.md). To access the final result, you need to use the `.value` attribute.

The loop records the initial input, `10`. When `increment()` returns `11`, the loop records that value and the input used to produce it. Each **version** records one value of an artifact and the inputs that produced it. In the `calculate()` function, `incr` holds the version for `11`. Passing it to `double` lets the loop record the link between the two stages. Inside the `double` stage, the argument is the ordinary integer `11`.

## Follow the result back to its input

In the same notebook, inspect the trace for the `calculate` invocation:

```python
print(loop.trace(calculate))
```

```text
start
start increment()
finish increment() -> calculate/increment/1@1
start double()
finish double() -> calculate/double/1@1
return calculate/double/1@1
```

Read `calculate/increment/1@1` from left to right. In the `calculate` invocation, the `increment` stage produced its first artifact (`/1`). This is its first version (`@1`). The version stores the value `11`, who produced it, and a link to the input version holding `10`. Revising it would keep the artifact number and advance the version to `@2`.

Use `print(loop.trace(double))` to see just the events for the double stage. The last two lines share a version ID because the flow returned the version produced by `double`. Follow its `number` input to find the intermediate result, then follow the same input on `increment` to reach the original value.

```python
parent = loop.input(answer, "number")
root = loop.input(parent, "number")

print(answer.actor, answer.value)  # double 22
print(parent.actor, parent.value)  # increment 11
print(root.actor, root.value)      # <input> 10
```

The actor label `<input>` marks a value supplied by the caller. Here it identifies the `10` passed to `loop.run`.
