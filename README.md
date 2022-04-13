# plugin-mzc-hyperbilling-cost-datasource
Plugin for collecting MEGAZONE HyperBilling data

---

## Supported Provider
* Google Cloud
* Alibaba
* Tencent
* Akamai

## Secret Data
*Schema*
* client_email (str): HyperBilling login E-mail 
* private_key (str): Credentials for authentication
* endpoint (str): MZC HyperBilling service endpoint
* account_id (str): Google Billing Account ID

*Example*
<pre>
<code>
{
    "client_id": "*****",
    "client_secret": "*****",
    "endpoint": "https://{url}
}
</code>
</pre>

## Options
Currently, not required.
