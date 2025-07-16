CREATE STAGE IF NOT EXISTS  DB_BNK_INDIA_{{env_var('SHORT_ENV')}}.SCH_EKIP_SL.STG_S3_INDIA_EKIP 	
    STORAGE_INTEGRATION = STI_S3_INDIA_{{env_var('SHORT_ENV')}}
    URL = 's3://s3b-dlz-{{env_var('ENVIRONMENT')}}-standard-india-ekip/' 
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = '{{env_var('KMS_KEY_ID')|lower}}');