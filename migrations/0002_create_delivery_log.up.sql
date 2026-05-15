-- delivery log table 

-- delivery status enum

CREATE TYPE delivery_status AS ENUM ('pending', 'success', 'failure');


-- delivery log table creation 
-- delivery id comes from X-Github-Delivery header, it must be unique cause it's our idempotency key 


CREATE TABLE IF NOT EXISTS delivery_log (
  id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  delivery_id UUID NOT NULL,
  webhook_id UUID NOT NULL REFERENCES webhooks(id) on DELETE CASCADE,
  attempt INT NOT NULL DEFAULT 1,
  status delivery_status NOT NULL DEFAULT 'pending',
  status_code INT,
  duration_ms INT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);




-- index for idempotent key's like - delivery id and attempt 

CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_log_delivery_attempt ON delivery_log (delivery_id, attempt);


-- index on delivery id of all attempts for fast lookups  

CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_log_delivery_id ON delivery_log (delivery_id);

-- index on webhook id for fast lookups

CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_log_webhook_id ON delivery_log (webhook_id);












