#!/usr/bin/env python3
import requests

base='http://171.244.195.150:8081'
paths=['/auth/login','/api/auth/login','/api/v1/auth/login','/auth/register','/api/auth/register','/api/v1/auth/register']
print('Checking register/login endpoints on', base)
for p in paths:
    url=base.rstrip('/')+p
    try:
        r=requests.post(url,json={'email':'dev+e2e@example.com','password':'Password123!'},timeout=10)
        print(p, '=>', r.status_code, r.text[:500].replace('\n',' '))
    except Exception as e:
        print(p, '=> error', e)
