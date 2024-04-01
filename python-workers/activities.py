from temporalio import activity


@activity.defn
async def say_hello(name: str) -> str:
    print(f"Start python activity with parameter: {name}!")
    return f"Hello, {name}!"
