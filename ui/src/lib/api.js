async function request(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(body.error || `${res.status} ${res.statusText}`);
  }
  return body;
}

export const getStats = () => request('/api/stats');
export const getDLQ = (limit = 50) => request(`/dlq?limit=${limit}`);
export const retryDLQ = (id) =>
  request('/dlq/retry', { method: 'POST', body: JSON.stringify({ id }) });
export const retryAllDLQ = () =>
  request('/dlq/retry', { method: 'POST', body: JSON.stringify({ all: true }) });
export const discardDLQ = (id) =>
  request('/dlq/discard', { method: 'POST', body: JSON.stringify({ id }) });
export const triggerBackfill = (table) =>
  request('/backfill', {
    method: 'POST',
    body: JSON.stringify(table ? { table } : { all: true }),
  });
