# from https://raw.githubusercontent.com/Azure/azure-sdk-for-python/master/sdk/servicebus/azure-servicebus/samples/async_samples/session_pool_receive_async.py
#!/usr/bin/env python

# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
import os
import asyncio
import uuid
import random

from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from azure.servicebus import ServiceBusMessage, NEXT_AVAILABLE_SESSION
from azure.servicebus.exceptions import OperationTimeoutError


CONNECTION_STR = os.getenv("SERVICEBUS_CONNECTION_STRING")
# Note: This must be a session-enabled queue.
queue_name = "foobar"



async def receive_messages(receiver):
    try:
            print(f"Starting handling session {receiver.session.session_id}")
            renewer = AutoLockRenewer()
            renewer.register(receiver, receiver.session)
            await receiver.session.set_state("OPEN")
            async for message in receiver:
                #print(f"{message.session_id}/{str(message)}")
                #print(repr(message))
                # print("Message: {}".format(message))
                # print("Time to live: {}".format(message.time_to_live))
                # print("Sequence number: {}".format(message.sequence_number))
                # print("Enqueue Sequence number: {}".format(message.enqueued_sequence_number))
                # #print("Partition Key: {}".format(message.partition_key))
                # print("Locked until: {}".format(message.locked_until_utc))
                # print("Lock Token: {}".format(message.lock_token))
                # print("Enqueued time: {}".format(message.enqueued_time_utc))
                await receiver.complete_message(message)
                if str(message) == 'shutdown':
                    await receiver.session.set_state("CLOSED")
                    print(f"Closed session {receiver.session.session_id}")
                    break
            await renewer.close()
    except OperationTimeoutError as ote:
        print("If timeout occurs during connecting to a session,"
                "It indicates that there might be no non-empty sessions remaining; exiting."
                "This may present as a UserError in the azure portal metric.", ote)
        return

async def message_processing(servicebus_client, count=2):
    workers = []
    async with servicebus_client.get_queue_receiver(queue_name, max_wait_time=1, session_id=NEXT_AVAILABLE_SESSION) as receiver:
        await asyncio.gather(*[receive_messages(receiver) for r in range(0, count)])
    print("Finished receiving")

async def create_sessions(client, count=1500):
    async with client.get_queue_sender(queue_name) as sender:
        async def sendsession():
                session_id = str(uuid.uuid4())
                print(f"Starting {session_id}")
                try:
                    await sender.send_messages(ServiceBusMessage("Start Message", session_id=session_id))
                    await asyncio.sleep(random.random())
                    await sender.send_messages(ServiceBusMessage("shutdown", session_id=session_id))
                    print(f"Finished: {session_id}")
                except Exception as e:
                    print("Problem: ", e)
        print("Creating Sessions")
        await asyncio.gather(*[sendsession() for i in range(0, count)])
        print("All senders finished")



async def sample_session_send_receive_with_pool_async(connection_string):
    try:
        client = ServiceBusClient.from_connection_string(connection_string)

        print(f"Built client: {client!r}")
        await asyncio.gather(create_sessions(client, 2000), message_processing(client, 2000), return_exceptions=True)

    except Exception as e:
        print("sample_session_send_receive_with_pool_async Problem", e)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sample_session_send_receive_with_pool_async(CONNECTION_STR))
