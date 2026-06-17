# Fixed Direct Jobs

`roman_aqueducts_city_engineering.fixed-job.json`
- Direct `script/generate-with-images` payload.
- Includes `skip_creator: true`, so the master enqueues it locally without calling the creator.
- Requires a valid full-length `voiceover_path` before submission.

`submit_roman_aqueducts_city_engineering.sh`
- Helper to submit the fixed job directly to the master.
- Usage:

```bash
./ops/jobs/submit_roman_aqueducts_city_engineering.sh "https://drive.google.com/file/d/<FULL_VOICEOVER_ID>/view"
```

Current reference assets bundled in the JSON:
- Scene image 1: `1QoPBq8z2DB9OUXyjIT3HwgKOYzihF8Mh`
- Scene image 2: `1S6NiFUeLEAQwtGZISX96nRsv6sv_p7f_`
- Reference per-scene voiceovers from creator output:
  - `11F0I60YScJN7tuVkpNhDeHzavHR7An9y`
  - `1z5_Tm7dSbu4tFKIEYpyVrdI7oWqy1dIR`

Important:
- The direct worker path still needs one valid full voiceover URL for the whole script.
- The two `reference_voiceovers` are saved as provenance, not as a guaranteed final mixed track.
