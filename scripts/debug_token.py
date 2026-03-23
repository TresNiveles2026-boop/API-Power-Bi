
import asyncio
import os
import httpx
import json
import base64
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
WS_ID = os.getenv("PBI_WORKSPACE_ID")
REP_ID = os.getenv("PBI_REPORT_ID")

def decode_jwt(token):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {"error": "Invalid token format"}
        padding = '=' * (4 - len(parts[1]) % 4)
        claims = json.loads(base64.b64decode(parts[1] + padding).decode('utf-8'))
        return claims
    except Exception as e:
        return {"error": str(e)}

async def main():
    print("================================================================")
    print("🔍 ADVANCED DIAGNOSTIC: 401 UNAUTHORIZED ANALYSIS")
    print("================================================================")
    
    # 1. Verification of Logic (User Request)
    print(f"1. CODE VALIDATION")
    print(f"   - Target Workspace ID from ENV: {WS_ID}")
    print(f"   - Target Report ID from ENV:    {REP_ID}")
    print(f"   - Security Group Name Used:     N/A (This script uses ServicePrincipal credentials directly)")
    print(f"   - Hardcoded 'PowerBI-Apps'?     NO. (Verified source code uses os.getenv)")

    # 2. Authentication (Entra ID)
    print(f"\n2. AUTHENTICATION (MsGraph/Entra ID)")
    app = ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    result = app.acquire_token_for_client(scopes=SCOPE)
    
    if "access_token" not in result:
        print(f"   ❌ Auth Failed: {result.get('error_description')}")
        return
        
    aad_token = result["access_token"]
    print(f"   ✅ Access Token Acquired.")
    
    # 3. Token Analysis
    print(f"\n3. TOKEN CLAIMS ANALYSIS (Decoding JWT)")
    claims = decode_jwt(aad_token)
    print(f"   - aud (Audience): {claims.get('aud')} (Should be https://analysis.windows.net/powerbi/api)")
    print(f"   - iss (Issuer):   {claims.get('iss')}")
    print(f"   - oid (Object ID): {claims.get('oid')} <--- CHECK IF THIS IS IN YOUR SECURITY GROUP")
    print(f"   - appid:          {claims.get('appid')}")
    print(f"   - tid (Tenant):   {claims.get('tid')}")

    # 4. API Call (Power BI)
    print(f"\n4. POWER BI API CALL (GenerateToken)")
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{WS_ID}/reports/{REP_ID}/GenerateToken"
    
    headers = {
        "Authorization": f"Bearer {aad_token}",
        "Content-Type": "application/json"
    }
    body = {"accessLevel": "View"}
    
    print(f"   ---------------- REQUEST ----------------")
    print(f"   POST {url}")
    print(f"   Headers: Authorization: Bearer [HIDDEN]...")
    print(f"            Content-Type: application/json")
    print(f"   Body:    {json.dumps(body)}")
    print(f"   -----------------------------------------")

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=body)
        
        print(f"\n   ---------------- RESPONSE ----------------")
        print(f"   STATUS: {resp.status_code} {resp.reason_phrase}")
        print(f"   HEADERS:")
        for k, v in resp.headers.items():
             print(f"      {k}: {v}")
        print(f"   BODY (RAW):")
        print(f"      {resp.text}")
        print(f"   ------------------------------------------")

if __name__ == "__main__":
    asyncio.run(main())
