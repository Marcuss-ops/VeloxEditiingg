#!/usr/bin/env python3
"""Verify the Golden E2E media contract for one exact artifact."""

import json
import subprocess
import sys
from fractions import Fraction


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    if len(sys.argv) != 8:
        fail("usage: golden-e2e-verify-media.py VIDEO WIDTH HEIGHT FPS DURATION SAMPLE_RATE CHANNELS")
    video, width, height, fps, duration, sample_rate, channels = sys.argv[1:]
    width, height, fps = int(width), int(height), float(fps)
    duration, sample_rate, channels = float(duration), int(sample_rate), int(channels)

    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", video],
        check=True,
        capture_output=True,
        text=True,
    )
    data = json.loads(probe.stdout)
    streams = data.get("streams", [])
    videos = [stream for stream in streams if stream.get("codec_type") == "video"]
    audios = [stream for stream in streams if stream.get("codec_type") == "audio"]
    if len(videos) != 1:
        fail(f"video stream count={len(videos)}, want 1")
    if len(audios) != 1:
        fail(f"audio stream count={len(audios)}, want 1")

    video_stream, audio_stream = videos[0], audios[0]
    if video_stream.get("codec_name") != "h264":
        fail(f"video codec={video_stream.get('codec_name')!r}, want h264")
    if (int(video_stream.get("width", 0)), int(video_stream.get("height", 0))) != (width, height):
        fail(f"resolution={video_stream.get('width')}x{video_stream.get('height')}, want {width}x{height}")
    try:
        actual_fps = float(Fraction(video_stream.get("r_frame_rate", "0/1")))
    except (ValueError, ZeroDivisionError):
        actual_fps = 0.0
    if abs(actual_fps - fps) > 0.05:
        fail(f"fps={actual_fps}, want {fps}")
    if audio_stream.get("codec_name") != "aac":
        fail(f"audio codec={audio_stream.get('codec_name')!r}, want aac")
    if int(audio_stream.get("sample_rate", 0)) != sample_rate:
        fail(f"audio sample_rate={audio_stream.get('sample_rate')}, want {sample_rate}")
    if int(audio_stream.get("channels", 0)) != channels or channels not in (1, 2):
        fail(f"audio channels={audio_stream.get('channels')}, want {channels}")

    def stream_duration(stream: dict) -> float:
        value = stream.get("duration")
        return float(value) if value else 0.0

    video_duration = stream_duration(video_stream) or float(data.get("format", {}).get("duration", 0.0))
    audio_duration = stream_duration(audio_stream)
    if video_duration < duration - 0.75:
        fail(f"video duration={video_duration:.3f}s too short for {duration}s")
    if audio_duration < duration - 0.75:
        fail(f"audio duration={audio_duration:.3f}s too short for {duration}s")
    if abs(video_duration - audio_duration) > 0.75:
        fail(f"A/V duration mismatch video={video_duration:.3f}s audio={audio_duration:.3f}s")

    volume = subprocess.run(
        ["ffmpeg", "-hide_banner", "-nostats", "-loglevel", "info", "-i", video,
         "-map", "0:a:0", "-af", "volumedetect", "-f", "null", "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    max_volume = next(
        (line.split(":", 1)[1].strip() for line in volume.stderr.splitlines() if "max_volume:" in line),
        "",
    )
    if not max_volume or float(max_volume.split()[0]) <= -60.0:
        fail(f"audio is effectively silent: max_volume={max_volume!r}")
    print(f"media: {video_duration:.3f}s video + {audio_duration:.3f}s audio, max_volume={max_volume}")


if __name__ == "__main__":
    main()
