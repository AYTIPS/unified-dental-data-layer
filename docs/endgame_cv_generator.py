# try:
#     await crm_client.some_call(...)
#     mark_crm_health_ok(clinic)
#     db.commit()
# except CrmAuthError as exc:
#     mark_crm_auth_failed(clinic, reason=str(exc))
#     db.commit()
#     raise

# put in the crm wrapper also 