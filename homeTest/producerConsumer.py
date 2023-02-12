import asyncio
import json
import os
import pandas as pd
import nats

SUBJECT_NAME = "csv_rows"
STREAM_NAME = "sample-stream"
# NATS Jetstream connection details
#local run:
# nats_jetstream = "nats://localhost:4222"

#docker run:
nats_jetstream = "nats://jetstream:4222"
context_set = set()


# Function to read a CSV file in chunks and send each row as a separate JSON event to the NATS broker
async def produce_csv(csv_file_path):
    # async with NATS() as nc:

    # Connect to NATS broker
    nc = await nats.connect(nats_jetstream)

    # Create JetStream context.
    js = nc.jetstream()

    # Persist messages on 'csv_rows's subject.
    await js.add_stream(name=STREAM_NAME, subjects=[SUBJECT_NAME])

    # Read the CSV file in chunks
    for chunk in pd.read_csv(csv_file_path, chunksize=100):
        # Convert each row to JSON and send as a separate event to the NATS broker
        for index, row in chunk.iterrows():
            await js.publish(SUBJECT_NAME, row.to_json().encode())


# Function to consume events from the NATS broker and write each row to a separate local file
async def consume_csv(consumer_id):
    # Connect to NATS broker
    nc = await nats.connect(nats_jetstream)
    js = nc.jetstream()
    count = 0
    global context_set

    # Subscribe to the "csv_rows" subject
    async def message_handler(msg):
        # Write the event (row) to a separate local file
        row = json.loads(msg.data.decode())
        df = pd.DataFrame([row])
        df.to_csv(f"consumer_{consumer_id}_rows.csv", mode='a', header=False, index=False)

    # sub = await js.subscribe(SUBJECT_NAME, durable=f"consumer{consumer_id}")
    sub = await js.subscribe(SUBJECT_NAME, "workers")

    # # Wait for completion
    while True:
        try:
            msg = await sub.next_msg()
            count += 1
            if msg.data.decode() not in context_set:
                await message_handler(msg)
                context_set.add(msg.data.decode())
            await msg.ack()
        except nats.errors.TimeoutError:
            break
    print(count)
    # Remove interest in subject
    await sub.unsubscribe()
    await nc.close()
    await combine_files()

    # Notify the other consumers and print log
    print(f"Consumer {consumer_id} has completed the task and combined the local files.")


async def combine_files():
    filenames = [filename for filename in os.listdir() if filename.startswith("consumer") and filename.endswith(".csv")]
    if filenames:
        with open('all_rows.csv', 'w') as fout:
            for filename in filenames:
                with open(os.path.join("./", filename)) as fin:
                    for line in fin:
                        fout.write(line)
                os.remove(filename)


# Main function to run the producer and consumer applications
async def main():
    csv_file_path = "sample.csv"

    # Run the producer application
    await produce_csv(csv_file_path)

    # Run the consumer applications in parallel
    tasks = [asyncio.create_task(consume_csv(i)) for i in range(3)]
    await asyncio.gather(*tasks)

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
