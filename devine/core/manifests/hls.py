from __future__ import annotations

import logging
import re
import shutil
import sys
import time
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from hashlib import md5
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Any, Callable, Optional, Union

import m3u8
import requests
from langcodes import Language
from m3u8 import M3U8
from pywidevine.cdm import Cdm as WidevineCdm
from pywidevine.pssh import PSSH
from requests import Session
from rich import filesize

from devine.core.constants import AnyTrack
from devine.core.downloaders import downloader, requests as requests_downloader
from devine.core.drm import DRM_T, ClearKey, Widevine
from devine.core.tracks import Audio, Subtitle, Tracks, Video
from devine.core.utilities import is_close_match


class HLS:
    def __init__(self, manifest: M3U8, session: Optional[Session] = None):
        if not manifest:
            raise ValueError("HLS manifest must be provided.")
        if not isinstance(manifest, M3U8):
            raise TypeError(f"Expected manifest to be a {M3U8}, not {manifest!r}")
        if not manifest.is_variant:
            raise ValueError("Expected the M3U(8) manifest to be a Variant Playlist.")

        self.manifest = manifest
        self.session = session or Session()

    @classmethod
    def from_url(cls, url: str, session: Optional[Session] = None, **args: Any) -> HLS:
        if not url:
            raise requests.URLRequired("HLS manifest URL must be provided.")
        if not isinstance(url, str):
            raise TypeError(f"Expected url to be a {str}, not {url!r}")

        if not session:
            session = Session()
        elif not isinstance(session, Session):
            raise TypeError(f"Expected session to be a {Session}, not {session!r}")

        res = session.get(url, **args)
        if not res.ok:
            raise requests.ConnectionError(
                "Failed to request the M3U(8) document.",
                response=res
            )

        master = m3u8.loads(res.text, uri=url)

        return cls(master, session)

    @classmethod
    def from_text(cls, text: str, url: str) -> HLS:
        if not text:
            raise ValueError("HLS manifest Text must be provided.")
        if not isinstance(text, str):
            raise TypeError(f"Expected text to be a {str}, not {text!r}")

        if not url:
            raise requests.URLRequired("HLS manifest URL must be provided for relative path computations.")
        if not isinstance(url, str):
            raise TypeError(f"Expected url to be a {str}, not {url!r}")

        master = m3u8.loads(text, uri=url)

        return cls(master)

    def to_tracks(self, language: Union[str, Language]) -> Tracks:
        """
        Convert a Variant Playlist M3U(8) document to Video, Audio and Subtitle Track objects.

        Parameters:
            language: Language you expect the Primary Track to be in.

        All Track objects' URL will be to another M3U(8) document. However, these documents
        will be Invariant Playlists and contain the list of segments URIs among other metadata.
        """
        session_drm = HLS.get_drm(self.manifest.session_keys)

        audio_codecs_by_group_id: dict[str, Audio.Codec] = {}
        tracks = Tracks()

        for playlist in self.manifest.playlists:
            url = playlist.uri
            if not re.match("^https?://", url):
                url = playlist.base_uri + url

            audio_group = playlist.stream_info.audio
            if audio_group:
                audio_codec = Audio.Codec.from_codecs(playlist.stream_info.codecs)
                audio_codecs_by_group_id[audio_group] = audio_codec

            try:
                # TODO: Any better way to figure out the primary track type?
                Video.Codec.from_codecs(playlist.stream_info.codecs)
            except ValueError:
                primary_track_type = Audio
            else:
                primary_track_type = Video

            tracks.add(primary_track_type(
                id_=md5(str(playlist).encode()).hexdigest()[0:7],  # 7 chars only for filename length
                url=url,
                codec=primary_track_type.Codec.from_codecs(playlist.stream_info.codecs),
                language=language,  # HLS manifests do not seem to have language info
                is_original_lang=True,  # TODO: All we can do is assume Yes
                bitrate=playlist.stream_info.average_bandwidth or playlist.stream_info.bandwidth,
                descriptor=Video.Descriptor.M3U,
                drm=session_drm,
                extra=playlist,
                # video track args
                **(dict(
                    range_=Video.Range.DV if any(
                        codec.split(".")[0] in ("dva1", "dvav", "dvhe", "dvh1")
                        for codec in playlist.stream_info.codecs.lower().split(",")
                    ) else Video.Range.from_m3u_range_tag(playlist.stream_info.video_range),
                    width=playlist.stream_info.resolution[0],
                    height=playlist.stream_info.resolution[1],
                    fps=playlist.stream_info.frame_rate
                ) if primary_track_type is Video else {})
            ))

        for media in self.manifest.media:
            url = media.uri
            if not url:
                continue

            if not re.match("^https?://", url):
                url = media.base_uri + url

            joc = 0
            if media.type == "AUDIO":
                track_type = Audio
                codec = audio_codecs_by_group_id.get(media.group_id)
                if media.channels and media.channels.endswith("/JOC"):
                    joc = int(media.channels.split("/JOC")[0])
                    media.channels = "5.1"
            else:
                track_type = Subtitle
                codec = Subtitle.Codec.WebVTT  # assuming WebVTT, codec info isn't shown

            tracks.add(track_type(
                id_=md5(str(media).encode()).hexdigest()[0:6],  # 6 chars only for filename length
                url=url,
                codec=codec,
                language=media.language or language,  # HLS media may not have language info, fallback if needed
                is_original_lang=language and is_close_match(media.language, [language]),
                descriptor=Audio.Descriptor.M3U,
                drm=session_drm if media.type == "AUDIO" else None,
                extra=media,
                # audio track args
                **(dict(
                    bitrate=0,  # TODO: M3U doesn't seem to state bitrate?
                    channels=media.channels,
                    joc=joc,
                    descriptive="public.accessibility.describes-video" in (media.characteristics or ""),
                ) if track_type is Audio else dict(
                    forced=media.forced == "YES",
                    sdh="public.accessibility.describes-music-and-sound" in (media.characteristics or ""),
                ) if track_type is Subtitle else {})
            ))

        return tracks

    @staticmethod
    def download_track(
        track: AnyTrack,
        save_path: Path,
        save_dir: Path,
        stop_event: Event,
        skip_event: Event,
        progress: partial,
        session: Optional[Session] = None,
        proxy: Optional[str] = None,
        license_widevine: Optional[Callable] = None
    ) -> None:
        if not session:
            session = Session()
        elif not isinstance(session, Session):
            raise TypeError(f"Expected session to be a {Session}, not {session!r}")

        if not track.needs_proxy and proxy:
            proxy = None

        if proxy:
            session.proxies.update({
                "all": proxy
            })

        log = logging.getLogger("HLS")

        master = m3u8.loads(
            # should be an invariant m3u8 playlist URI
            session.get(track.url).text,
            uri=track.url
        )

        if not master.segments:
            log.error("Track's HLS playlist has no segments, expecting an invariant M3U8 playlist.")
            sys.exit(1)

        init_data_groups = HLS.group_segments(master.segments, track, license_widevine, proxy, session)

        if skip_event.is_set():
            return

        progress(total=len(master.segments))

        range_offset = Queue(maxsize=1)
        range_offset.put(0)
        download_sizes = []
        download_speed_window = 5
        last_speed_refresh = time.time()

        with ThreadPoolExecutor(max_workers=16) as pool:
            for g1, (init_data, segment_groups) in enumerate(init_data_groups):
                for g2, (drm, segments) in enumerate(segment_groups):
                    if not segments:
                        continue
                    out_path_root = save_dir / f"group_{g1:{len(str(g1))}}_{g2:{len(str(g2))}}"
                    out_path_root.mkdir(parents=True, exist_ok=True)
                    group_save_path = (save_dir / out_path_root.name).with_suffix(".mp4")
                    for i, download in enumerate(futures.as_completed((
                        pool.submit(
                            HLS.download_segment,
                            segment=segment,
                            out_path=(out_path_root / str(n).zfill(len(str(len(master.segments))))).with_suffix(".mp4"),
                            track=track,
                            range_offset=range_offset,
                            session=session,
                            proxy=proxy,
                            stop_event=stop_event
                        )
                        for n, segment in enumerate(segments)
                    ))):
                        try:
                            download_size = download.result()
                        except KeyboardInterrupt:
                            stop_event.set()  # skip pending track downloads
                            progress(downloaded="[yellow]STOPPING")
                            pool.shutdown(wait=True, cancel_futures=True)
                            progress(downloaded="[yellow]STOPPED")
                            # tell dl that it was cancelled
                            # the pool is already shut down, so exiting loop is fine
                            raise
                        except Exception as e:
                            stop_event.set()  # skip pending track downloads
                            progress(downloaded="[red]FAILING")
                            pool.shutdown(wait=True, cancel_futures=True)
                            progress(downloaded="[red]FAILED")
                            # tell dl that it failed
                            # the pool is already shut down, so exiting loop is fine
                            raise e
                        else:
                            # it successfully downloaded, and it was not cancelled
                            progress(advance=1)

                            now = time.time()
                            time_since = now - last_speed_refresh

                            if download_size:  # no size == skipped dl
                                download_sizes.append(download_size)

                            if download_sizes and (time_since > download_speed_window or i == len(master.segments)):
                                data_size = sum(download_sizes)
                                download_speed = data_size / (time_since or 1)
                                progress(downloaded=f"HLS {filesize.decimal(download_speed)}/s")
                                last_speed_refresh = now
                                download_sizes.clear()

                    with open(group_save_path, "wb") as f:
                        if init_data:
                            f.write(init_data)
                        for segment_file in sorted(out_path_root.iterdir()):
                            f.write(segment_file.read_bytes())
                    shutil.rmtree(out_path_root)

                    if drm:
                        drm.decrypt(group_save_path)
                        track.drm = None
                        if callable(track.OnDecrypted):
                            track.OnDecrypted(track)

        with open(save_path, "wb") as f:
            for group_file in sorted(save_dir.iterdir()):
                f.write(group_file.read_bytes())
                group_file.unlink()

        progress(downloaded="Downloaded")

        track.path = save_path
        save_dir.rmdir()

    @staticmethod
    def download_segment(
        segment: m3u8.Segment,
        out_path: Path,
        track: AnyTrack,
        range_offset: Queue,
        session: Optional[Session] = None,
        proxy: Optional[str] = None,
        stop_event: Optional[Event] = None
    ) -> int:
        """
        Download an HLS Media Segment.

        Parameters:
            segment: The m3u8.Segment Object to Download.
            out_path: Path to save the downloaded Segment file to.
            track: The Track object of which this Segment is for. Currently used to fix an
                invalid value in the TFHD box of Audio Tracks, and for the Segment Filter.
            range_offset: Queue for saving and loading the last-used Byte Range offset.
            proxy: Proxy URI to use when downloading the Segment file.
            session: Python-Requests Session used when requesting init data.
            stop_event: Prematurely stop the Download from beginning. Useful if ran from
                a Thread Pool. It will raise a KeyboardInterrupt if set.

        Returns the file size of the downloaded Segment in bytes.
        """
        if stop_event.is_set():
            raise KeyboardInterrupt()

        if callable(track.OnSegmentFilter) and track.OnSegmentFilter(segment):
            return 0

        if not segment.uri.startswith(segment.base_uri):
            segment.uri = segment.base_uri + segment.uri

        attempts = 1
        while True:
            try:
                headers_ = session.headers
                if segment.byterange:
                    # aria2(c) doesn't support byte ranges, use python-requests
                    downloader_ = requests_downloader
                    previous_range_offset = range_offset.get()
                    byte_range = HLS.calculate_byte_range(segment.byterange, previous_range_offset)
                    range_offset.put(byte_range.split("-")[0])
                    headers_["Range"] = f"bytes={byte_range}"
                else:
                    downloader_ = downloader
                downloader_(
                    uri=segment.uri,
                    out=out_path,
                    headers=headers_,
                    proxy=proxy,
                    silent=attempts != 5,
                    segmented=True
                )
                break
            except Exception as ee:
                if stop_event.is_set() or attempts == 5:
                    raise ee
                time.sleep(2)
                attempts += 1

        download_size = out_path.stat().st_size

        # fix audio decryption on ATVP by fixing the sample description index
        # TODO: Should this be done in the video data or the init data?
        if isinstance(track, Audio):
            with open(out_path, "rb+") as f:
                segment_data = f.read()
                fixed_segment_data = re.sub(
                    b"(tfhd\x00\x02\x00\x1a\x00\x00\x00\x01\x00\x00\x00)\x02",
                    b"\\g<1>\x01",
                    segment_data
                )
                if fixed_segment_data != segment_data:
                    f.seek(0)
                    f.write(fixed_segment_data)

        return download_size

    @staticmethod
    def get_drm(
        keys: list[Union[m3u8.model.SessionKey, m3u8.model.Key]],
        proxy: Optional[str] = None
    ) -> list[DRM_T]:
        """
        Convert HLS EXT-X-KEY data to initialized DRM objects.

        Only EXT-X-KEY methods that are currently supported will be returned.
        The rest will simply be ignored, unless none of them were supported at
        which it will raise a NotImplementedError.

        If an EXT-X-KEY with the method `NONE` is passed, then an empty list will
        be returned. EXT-X-KEY METHOD=NONE means from hence forth the playlist is
        no longer encrypted, unless another EXT-X-KEY METHOD is set later.

        Parameters:
            keys: List of Segment Keys or Playlist Session Keys.
            proxy: Proxy URI to use when downloading Clear-Key DRM URIs (e.g. AES-128).
        """
        drm = []
        unsupported_systems = []

        for key in keys:
            if not key:
                continue
            # TODO: Add support for 'SAMPLE-AES', 'AES-CTR', 'AES-CBC', 'ClearKey'
            if key.method == "NONE":
                return []
            elif key.method == "AES-128":
                drm.append(ClearKey.from_m3u_key(key, proxy))
            elif key.method == "ISO-23001-7":
                drm.append(Widevine(
                    pssh=PSSH.new(
                        key_ids=[key.uri.split(",")[-1]],
                        system_id=PSSH.SystemId.Widevine
                    )
                ))
            elif key.keyformat and key.keyformat.lower() == WidevineCdm.urn:
                drm.append(Widevine(
                    pssh=PSSH(key.uri.split(",")[-1]),
                    **key._extra_params  # noqa
                ))
            else:
                unsupported_systems.append(key.method + (f" ({key.keyformat})" if key.keyformat else ""))

        if not drm and unsupported_systems:
            raise NotImplementedError(f"No support for any of the key systems: {', '.join(unsupported_systems)}")

        return drm

    @staticmethod
    def calculate_byte_range(m3u_range: str, fallback_offset: int = 0) -> str:
        """
        Convert a HLS EXT-X-BYTERANGE value to a more traditional range value.
        E.g., '1433@0' -> '0-1432', '357392@1433' -> '1433-358824'.
        """
        parts = [int(x) for x in m3u_range.split("@")]
        if len(parts) != 2:
            parts.append(fallback_offset)
        length, offset = parts
        return f"{offset}-{offset + length - 1}"

    @staticmethod
    def group_segments(
        segments: m3u8.SegmentList,
        track: AnyTrack,
        license_widevine: Optional[Callable] = None,
        proxy: Optional[str] = None,
        session: Optional[Session] = None
    ) -> list[tuple[Optional[bytes], list[tuple[Optional[DRM_T], list[m3u8.Segment]]]]]:
        """
        Group Segments that can be decoded and decrypted with the same information.
        It also initializes both the init data and DRM information.

        Since HLS allows you to set, remove, or change Segment Keys and Init Data at any
        point at a per-segment level, we need a way to know when we have a set of segments
        ready to be initialized with the same data, and decrypted with the same key.

        One way of doing this is by simply doing a linear comparison and blocking them into
        lists, which is exactly what this is doing. It groups segments with matching init
        data, then within that list, it groups segments with matching keys:

            groups = [(
                initData, [(
                    segmentKey, [
                        segment, ...
                    ]
                ), ...]
            ), ...]

        Both initData and segmentKey may be set to `None`. The segmentKey may be set to None
        if no EXT-X-KEY has been encountered yet, or if an EXT-X-KEY of METHOD=NONE is
        encountered. The initData may be None if the HLS manifest doesn't require or use
        init data, e.g., transport stream manifests. It is not an error for either or even
        both of them to be None.

        Parameters:
            segments: A list of Segments from an M3U8.
            track: Track to retrieve the initial Session Keys from, and to store the
                latest Keys in for Services to use.
            license_widevine: Function used to license Widevine DRM objects. It must be passed
                if the Segment's DRM uses Widevine.
            proxy: Proxy URI to use when downloading Clear-Key DRM URIs (e.g. AES-128) and
                when retrieving the Segment's init data.
            session: Python-Requests Session used when retrieving the Segment's init data.
        """
        current_init_data = None
        current_segment_keys = None
        current_segment_drm = None

        init_data_group_i = 0
        drm_key_group_i = 0

        # the variant master playlist had session DRM data
        if track.drm:
            # TODO: What if we don't want to use the first DRM system?
            session_drm = track.drm[0]
            if isinstance(session_drm, Widevine):
                # license and grab content keys
                if not license_widevine:
                    raise ValueError("license_widevine func must be supplied to use Widevine DRM")
                license_widevine(session_drm)
            current_segment_drm = session_drm

        init_data_groups: list[tuple[Optional[bytes], list[tuple[Optional[DRM_T], list[m3u8.Segment]]]]] = \
            [(None, [(current_segment_drm, [])])]

        for i, segment in enumerate(segments):
            if segment.init_section and (current_init_data is None or segment.discontinuity):
                # Only use the init data if there's no init data yet (i.e., start of file)
                # or if EXT-X-DISCONTINUITY is reached at the same time as EXT-X-MAP.
                # Even if a new EXT-X-MAP is supplied, it may just be duplicate and would
                # be unnecessary and slow to re-download the init data each time.
                if not segment.init_section.uri.startswith(segment.init_section.base_uri):
                    segment.init_section.uri = segment.init_section.base_uri + segment.init_section.uri

                if segment.init_section.byterange:
                    byte_range = HLS.calculate_byte_range(segment.init_section.byterange)
                    range_header = {
                        "Range": f"bytes={byte_range}"
                    }
                else:
                    range_header = {}

                res = session.get(
                    url=segment.init_section.uri,
                    headers=range_header,
                    proxies={"all": proxy} if proxy else None
                )
                res.raise_for_status()
                current_init_data = res.content

                init_data_groups.append((current_init_data, [(current_segment_drm, [])]))
                init_data_group_i += 1
                drm_key_group_i = 0

            if segment.keys and current_segment_keys != segment.keys:
                current_segment_keys = segment.keys
                drm = HLS.get_drm(
                    keys=current_segment_keys,
                    proxy=proxy
                )
                if drm:
                    track.drm = drm
                    # license and grab content keys
                    # TODO: What if we don't want to use the first DRM system?
                    drm = drm[0]
                    if isinstance(drm, Widevine):
                        track_kid = track.get_key_id(current_init_data)
                        if not license_widevine:
                            raise ValueError("license_widevine func must be supplied to use Widevine DRM")
                        license_widevine(drm, track_kid=track_kid)
                if current_segment_drm != drm:
                    current_segment_drm = drm
                    init_data_groups[init_data_group_i][1].append((current_segment_drm, []))
                    drm_key_group_i += 1

            init_data_groups[init_data_group_i][1][drm_key_group_i][1].append(segment)

        return init_data_groups


__ALL__ = (HLS,)
