export type VatRow = {
  input?: string;
  source?: string;
  state?: string; // valid/invalid/queued/retry/processing/error

  vat_number?: string;    // "FR123..."
  country_code?: string;  // "FR"
  vat_part?: string;      // zonder prefix

  valid?: boolean | null;
  name?: string;
  address?: string;

  // nieuw
  format_ok?: boolean;
  format_reason?: string;

  requester?: string; // bijv "NL123..."
  checked_at?: number;

  attempt?: number;        // retry attempt count
  next_retry_at?: number;  // epoch ms
  error_code?: string;     // bijv MS_MAX_CONCURRENT_REQ
  error?: string;
  details?: string;

  note?: string;          // user note
  tag?: "whitelist" | "blacklist" | ""; // user tag
  case_ref?: string;      // batch context
};

export type ValidateBatchRequest = {
  vat_numbers: string[];
  case_ref?: string;
};

export type ValidateBatchResponse = {
  count: number;
  fr_job_id?: string | null;
  duplicates_ignored?: number;
  results: VatRow[];
  vies_status?: Array<{ countryCode: string; availability: string }>;
};

export type FrJobResponse = {
  job: {
    job_id: string;
    status: string; // running/completed/queued
    total: number;
    done: number;
    updated_at: number;
    created_at: number;
    message?: string | null;
  };
  results: VatRow[];
};

export type ViesStatusResponse = {
  countries: Array<{ countryCode: string; availability: string }>;
};
