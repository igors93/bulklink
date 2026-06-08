from __future__ import annotations

import asyncio

from bulklink import PartitionedBulkhead


async def fetch_for_customer(customer_id: str, item: int) -> str:
    await asyncio.sleep(0.01)
    return f"{customer_id}:{item}"


async def main() -> None:
    customers = PartitionedBulkhead(
        label="customers",
        parallelism=2,
        waiting_room=4,
        wait_limit=1.0,
        max_partitions=100,
        idle_timeout=60.0,
    )

    results = await asyncio.gather(
        customers.execute("customer-a", fetch_for_customer, "customer-a", 1),
        customers.execute("customer-a", fetch_for_customer, "customer-a", 2),
        customers.execute("customer-b", fetch_for_customer, "customer-b", 3),
    )

    status = await customers.status()
    print(results)
    print("partitions", status.partition_count)
    print("active", status.active_partitions)

    await customers.close_and_wait()


if __name__ == "__main__":
    asyncio.run(main())
