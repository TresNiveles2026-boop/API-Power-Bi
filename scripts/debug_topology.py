
import asyncio
import os
import httpx
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
WS_ID = os.getenv("PBI_WORKSPACE_ID")
REP_ID = os.getenv("PBI_REPORT_ID")

async def main():
    print("================================================================")
    print("🔍 TOPOLOGY DIAGNOSTIC: DATASET & RLS CHECK")
    print("================================================================")

    # 1. Auth
    app = ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        print(f"❌ Auth Failed: {result.get('error_description')}")
        return
    token = result["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    async with httpx.AsyncClient() as client:
        # 2. Get Report Details
        print(f"\n1. INSPECTING REPORT: {REP_ID}")
        rep_url = f"https://api.powerbi.com/v1.0/myorg/groups/{WS_ID}/reports/{REP_ID}"
        resp_rep = await client.get(rep_url, headers=headers)
        
        if resp_rep.status_code != 200:
             print(f"❌ Failed to get report: {resp_rep.status_code}")
             print(resp_rep.text)
             return

        rep_data = resp_rep.json()
        dataset_id = rep_data.get("datasetId")
        print(f"   ✅ Report Found: {rep_data.get('name')}")
        print(f"   📄 Dataset ID:   {dataset_id}")
        print(f"   📄 Report WebUrl: {rep_data.get('webUrl')}")

        # 3. Get Dataset Details (Try Same Workspace first)
        print(f"\n2. INSPECTING DATASET: {dataset_id}")
        ds_url = f"https://api.powerbi.com/v1.0/myorg/groups/{WS_ID}/datasets/{dataset_id}"
        resp_ds = await client.get(ds_url, headers=headers)
        
        if resp_ds.status_code != 200:
            print(f"⚠️  Dataset NOT found in the SAME Workspace ({resp_ds.status_code})")
            print(f"    Searching other workspaces...")
            # Fallback: List all workspaces to find dataset? (Expensive, maybe logical next step if needed)
            # For now, assume it's missing permissions or cross-workspace.
        else:
            ds_data = resp_ds.json()
            print(f"   ✅ Dataset Found in SAME Workspace.")
            print(f"   🏷️  Name: {ds_data.get('name')}")
            print(f"   💾 ConfiguredBy: {ds_data.get('configuredBy')}")
            print(f"   🔒 IsEffectiveIdentityRequired (RLS): {ds_data.get('isEffectiveIdentityRequired')}")
            print(f"   👥 IsEffectiveIdentityRolesRequired: {ds_data.get('isEffectiveIdentityRolesRequired')}")
            print(f"   🏗️  TargetStorageMode: {ds_data.get('targetStorageMode')}")
            
            if ds_data.get('isEffectiveIdentityRequired') == True:
                print(f"\n   🛑 CRITICAL: RLS DETECTED!")
                print(f"   You MUST provide 'identities' in the GenerateToken payload.")
                print(f"   Current Payload sends ONLY 'accessLevel': 'View'. This will fail.")

    # 4. Check API Access to GenerateToken (Dry Run with different payload if RLS)
    # Refrain from calling it here to avoid noise, user wants analysis first.

if __name__ == "__main__":
    asyncio.run(main())
