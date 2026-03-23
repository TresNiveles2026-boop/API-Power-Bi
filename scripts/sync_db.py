import asyncio
import os
from dotenv import load_dotenv
from app.db.supabase_client import get_supabase_client

# Cargar .env
load_dotenv()

PBI_WORKSPACE_ID = os.getenv("PBI_WORKSPACE_ID").strip('"')
PBI_REPORT_ID = os.getenv("PBI_REPORT_ID").strip('"')
DEMO_REPORT_ID = "94e97143-fcba-4d04-b871-9e4e3b0c65ed"  # Hardcoded in frontend page.tsx

async def main():
    print(f"Syncing PBI credentials to DB...")
    print(f"Workspace: {PBI_WORKSPACE_ID}")
    print(f"Report: {PBI_REPORT_ID}")

    client = get_supabase_client()
    
    # Update the demo report record with real PBI IDs
    response = (
        client.table("reports")
        .update({
            "pbi_workspace_id": PBI_WORKSPACE_ID,
            "pbi_report_id": PBI_REPORT_ID,
            "pbi_dataset_id": "dummy_dataset_id"  # Not critical for embedding, but needed for schema
        })
        .eq("id", DEMO_REPORT_ID)
        .execute()
    )
    
    if response.data:
        print("✅ Database updated successfully!")
    else:
        print("❌ Failed to update database. Check if record exists.")

if __name__ == "__main__":
    asyncio.run(main())
