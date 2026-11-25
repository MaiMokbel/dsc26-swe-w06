# AI usage

## Tools


I used Claude for this task. It showed me structure of the first version of app/budget.py, app/stream.py app/main.py, and tests/test_mine.py the structure not the code 
. I ran the public tests and those tests locally (29 passing) .

## What AI got wrong

The first version put the whole stream inside `async with asyncio.timeout(...)`. The README
says this is a trap because the timeout can fire while the consumer is reading, not the
provider. 

## In my own words

1. Why is it safe to switch provider before the first token and not after?

   Before the first token the user has seen nothing, so another provider can start the answer
   from zero and nobody notices. After the first token the user already has half an answer
   from one model, and a second model would write a different answer, so the text would not
   fit together.

2. Which checks did you put before the stream starts, and why could they not be error events instead?

   Input validation (clause length, language, tenant header) gives 422, and the budget check
   gives 429 if the cheapest provider is too expensive. They have to happen before streaming
   because once the stream starts the status is already 200 and cannot change, so the client
   would think the request worked.