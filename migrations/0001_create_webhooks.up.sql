-- webhooks postgres table  

-- it will install pgcrypto extension 
-- pgcrypto is and utility extensions for cryptographic functions 

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- webhooks table  

CREATE TABLE IF NOT EXISTS webhooks (
   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
   target_url  TEXT NOT NULL,
   secret TEXT NOT NULL,
   events TEXT[] NOT NULL DEFAULT '{}',
   active BOOLEAN NOT NULL DEFAULT true,
   created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
   updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);



-- index on active webhooks for fast lookups  

CREATE INDEX IF NOT EXISTS idx_webhooks_active ON webhooks (active);




