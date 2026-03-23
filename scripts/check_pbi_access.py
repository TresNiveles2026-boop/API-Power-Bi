
import asyncio
import os
import httpx
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

# Cargar .env
load_dotenv()

CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

async def main():
    print(f"🔍 Diagnóstico de Acceso Power BI")
    print(f"--------------------------------")
    print(f"Client ID: {CLIENT_ID}")
    print(f"Tenant ID: {TENANT_ID}")
    
    # 1. Autenticación MSAL
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    
    if "access_token" not in result:
        print(f"❌ Error de Autenticación Azure AD: {result.get('error_description')}")
        return

    token = result["access_token"]
    print(f"✅ Autenticación Azure AD exitosa (Token obtenido)")

    # 2. Listar Workspaces
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.powerbi.com/v1.0/myorg/groups"
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        
        if resp.status_code != 200:
            print(f"❌ Error al listar Workspaces: {resp.status_code}")
            print(f"Respuesta: {resp.text}")
            if resp.status_code == 403:
                print("⚠️  CAUSA PROBABLE: Admin Portal 'Allow service principals' está DESACTIVADO.")
            return

        data = resp.json()
        groups = data.get("value", [])
        
        print(f"📊 Workspaces encontrados: {len(groups)}")
        
        target_ws_id = os.getenv("PBI_WORKSPACE_ID")
        found = False
        
        for g in groups:
            print(f" - [{g['id']}] {g['name']} (IsOnDedicatedCapacity: {g.get('isOnDedicatedCapacity')})")
            if g['id'] == target_ws_id:
                found = True
        
        print(f"--------------------------------")
        if found:
            print(f"✅ TU WORKSPACE ({target_ws_id}) ESTÁ EN LA LISTA.")
            
            # 3. Listar Reportes en el Workspace
            print(f"--------------------------------")
            print(f"🔍 Intentando listar reportes en el Workspace...")
            reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{target_ws_id}/reports"
            resp_rep = await client.get(reports_url, headers=headers)
            
            if resp_rep.status_code == 200:
                reports = resp_rep.json().get("value", [])
                print(f"✅ Reportes encontrados: {len(reports)}")
                rep_found = False
                target_rep_id = os.getenv("PBI_REPORT_ID")
                for r in reports:
                     print(f" - [{r['id']}] {r['name']}")
                     print(f"   -> DatasetId: {r.get('datasetId')}")
                     print(f"   -> EmbedUrl:  {r.get('embedUrl') is not None}")
                     print(f"   -> WebUrl:    {r.get('webUrl')}")
                     
                     if r['id'] == target_rep_id:
                         rep_found = True
                         dataset_id = r.get('datasetId')
                
                if rep_found:
                    print(f"✅ TU REPORTE ({target_rep_id}) EXISTE y es visible.")
                    print("CONCLUSION: Tienes acceso, pero 'GenerateToken' falla.")
                    print("CAUSA PROBABLE: El Workspace no está asignado a la CAPACIDAD FABRIC/PREMIUM.")
                else:
                    print(f"❌ EL REPORTE {target_rep_id} NO APARECE en la lista.")
            else:
                print(f"❌ Error al listar reportes: {resp_rep.status_code}")
                print("CAUSA: Permisos insuficientes en el Workspace (¿Eres solo Viewer?).")

        else:
            print(f"❌ TU WORKSPACE ({target_ws_id}) NO APARECE.")

if __name__ == "__main__":
    asyncio.run(main())
