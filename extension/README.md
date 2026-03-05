# JD Resume Autotailor Extension (MV3)

## Load locally

1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked** and select the `extension/` folder

## Configure

- `API Base URL`: defaults to `http://localhost:8000`
- `Profile ID`: profile row ID from `/api/profile`

## Flow

1. Open a job posting tab.
2. Click extension icon to open side panel.
3. Click **Capture JD**.
4. Review/edit JD text if needed.
5. Click **Generate PDF**.
6. Click **Download PDF**.

## Notes

- Extraction can fail on heavily dynamic/iframe pages. Use manual paste fallback.
- Backend endpoint must allow CORS from extension (`CORS_ALLOW_ORIGINS=*` in dev).
