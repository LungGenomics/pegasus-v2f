# PEGASUS V2F — UI

*Status: planned*

React web interface for browsing PEGASUS V2F databases. Served by the API server (`v2f serve`).

## Planned features

- Gene search with typeahead
- Evidence browser (by gene, by locus, by trait)
- Locus visualization with candidate gene rankings
- Data source management (import, integrate, remove)
- PEGASUS export download

## Development

The UI is served as static files by the FastAPI server. During development, the React dev server can proxy API requests to the backend.

```bash
# Start the API server
v2f serve --reload

# In another terminal, start the React dev server
cd ui
npm install
npm run dev
```
