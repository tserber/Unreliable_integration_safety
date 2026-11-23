"""
The original, unreliable implementation, as given in the take-home brief.
Kept verbatim for reference/diff purposes - this is the "primary code"
being replaced, not something this repo runs or tests. It has no schema,
no queue implementation, and no error handling of its own; `db` and
`queue` here are the undefined externals the brief described.

See DESIGN.md section 1 ("Diagnosing the original design") for exactly
which production incident each gap below caused, and the rest of
DESIGN.md plus app/ for the replacement.
"""


async def accept_handoff(company_id, payload):
    if not await db.event_exists(payload["event_id"]):
        request = await db.insert_handoff(company_id, payload)
        await queue.publish({"handoff_id": request.id})
    return {"accepted": True}


async def process_handoff(handoff_id):
    request = await db.get_handoff(handoff_id)

    if request.status == "completed":
        return

    result = await crm.create_callback(request.payload)
    await db.mark_completed(request.id, result.id)
