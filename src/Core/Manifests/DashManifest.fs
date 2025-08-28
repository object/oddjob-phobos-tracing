namespace Nrk.Oddjob.Core.Manifests

type DashManifestStream =
    {
        Bandwidth: int
        Resolution: int * int
        FrameRate: decimal
        Codecs: string
    }

type DashManifestMedia =
    {
        MimeType: string
        Content: string list
    }

type DashManifest =
    {
        Streams: DashManifestStream list
        Media: DashManifestMedia list
    }

    static member Zero = { Streams = []; Media = [] }

module DashManifest =

    open System
    open FSharp.Data
    open Nrk.Oddjob.Core

    type private VodDashXml =
        XmlProvider<"""
      <MPD xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dolby="http://www.dolby.com/ns/online/DASH" xmlns="urn:mpeg:dash:schema:mpd:2011" xsi:schemaLocation="urn:mpeg:dash:schema:mpd:2011 http://standards.iso.org/ittf/PubliclyAvailableStandards/MPEG-DASH_schema_files/DASH-MPD.xsd" xmlns:cenc="urn:mpeg:cenc:2013" xmlns:mspr="urn:microsoft:playready" profiles="urn:mpeg:dash:profile:isoff-live:2011,urn:com:dashif:dash264" minBufferTime="PT10S" type="static" mediaPresentationDuration="PT58M00S">
        <Period start="PT0.000S" id="8778696" duration="PT29.229S">
          <AdaptationSet mimeType="video/mp4" segmentAlignment="true" startWithSAP="1" subsegmentAlignment="true" subsegmentStartsWithSAP="1" bitstreamSwitching="true">
            <Representation id="1" width="960" height="540" frameRate="30000/1001" bandwidth="2200000" codecs="avc1.640029">
              <SegmentTemplate timescale="30000" media="index_video_7_0_$Number$.mp4?m=1566416213" initialization="index_video_7_0_init.mp4?m=1566416213" startNumber="8778700" presentationTimeOffset="1317997547283">
                <SegmentTimeline>
                  <S t="1317997547283" d="180180" r="3"/>
                  <S t="1317998268003" d="156156"/>
                </SegmentTimeline>
              </SegmentTemplate>
            </Representation>
            <Representation id="2" width="1280" height="720" frameRate="30000/1001" bandwidth="3299968" codecs="avc1.640029">
              <SegmentTemplate timescale="30000" media="index_video_10_0_$Number$.mp4?m=1566416213" initialization="index_video_10_0_init.mp4?m=1566416213" startNumber="8778700" presentationTimeOffset="1317997547283">
                <SegmentTimeline>
                  <S t="1317997547283" d="180180" r="3"/>
                  <S t="1317998268003" d="156156"/>
                </SegmentTimeline>
              </SegmentTemplate>
            </Representation>
          </AdaptationSet>
          <AdaptationSet mimeType="audio/mp4" segmentAlignment="0" lang="eng">
            <Label>eng</Label>
            <Representation id="4" bandwidth="96636" audioSamplingRate="48000" codecs="mp4a.40.2">
              <AudioChannelConfiguration schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011" value="2"></AudioChannelConfiguration>
              <SegmentTemplate timescale="48000" media="index_audio_5_0_$Number$.mp4?m=1566416213" initialization="index_audio_5_0_init.mp4?m=1566416213" startNumber="8778700" presentationTimeOffset="2108796075909">
                <SegmentTimeline>
                  <S t="2108796075909" d="288768"/>
                  <S t="2108796364677" d="287744"/>
                </SegmentTimeline>
              </SegmentTemplate>
            </Representation>
            <Representation id="5" bandwidth="96636" audioSamplingRate="48000" codecs="mp4a.40.2">
              <AudioChannelConfiguration schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011" value="2"></AudioChannelConfiguration>
              <SegmentTemplate timescale="48000" media="index_audio_8_0_$Number$.mp4?m=1566416213" initialization="index_audio_8_0_init.mp4?m=1566416213" startNumber="8778700" presentationTimeOffset="2108796075909">
                <SegmentTimeline>
                  <S t="2108796075909" d="288768"/>
                  <S t="2108796364677" d="287744"/>
                </SegmentTimeline>
              </SegmentTemplate>
            </Representation>
          </AdaptationSet>
          <AdaptationSet mimeType="application/mp4" codecs="stpp" segmentAlignment="true" startWithSAP="1" bitstreamSwitching="true" lang="eng">
            <Label>eng</Label>
            <Representation id="7" bandwidth="0">
              <SegmentTemplate timescale="90000" media="index_subtitles_4_0_$Number$.mp4?m=1566416213" initialization="index_subtitles_4_0_init.mp4?m=1566416213" startNumber="8778700" presentationTimeOffset="3953992641850">
                <SegmentTimeline>
                  <S t="3953992641850" d="540540" r="3"/>
                  <S t="3953994804010" d="468468"/>
                </SegmentTimeline>
              </SegmentTemplate>
            </Representation>
          </AdaptationSet>
          <AdaptationSet group="3" mimeType="text/vtt" contentType="text" lang="no">
           <SegmentTemplate timescale="90000" media="$RepresentationID$-0?version_hash=9ca06980">
            <SegmentTimeline/>
           </SegmentTemplate>
           <Role schemeIdUri="urn:mpeg:dash:role:2011" value="subtitle"/>
           <Representation id="1224817305" bandwidth="100">
            <BaseURL>https://undertekst.nrk.no/prod/KOIF22/00/KOIF22003618BA/NOR/KOIF22003618BA-v2.vtt</BaseURL>
           </Representation>
          </AdaptationSet>
        </Period>
        <Period start="PT0.000S" id="8778696" duration="PT29.229S">
        </Period>
      </MPD>
      """>

    let parse (text: string) =
        let mpd = VodDashXml.Parse text
        let streams, media =
            mpd.Periods
            |> Array.map (_.AdaptationSets)
            |> Array.concat
            |> Array.partition (fun set -> set.MimeType = "video/mp4" || set.MimeType = "audio/mp4")
        let streams =
            streams
            |> Array.map (_.Representations)
            |> Array.concat
            |> Array.map (fun r ->
                {
                    Bandwidth = r.Bandwidth
                    Resolution = (r.Width |> Option.defaultValue 0, r.Height |> Option.defaultValue 0)
                    FrameRate = r.FrameRate |> Option.map Convert.ToDecimal |> Option.defaultValue 0m
                    Codecs = r.Codecs |> Option.defaultValue null
                })
            |> Array.toList
        let media =
            media
            |> Array.map (fun set ->
                {
                    MimeType = set.MimeType
                    Content =
                        set.Representations
                        |> Array.choose (fun r ->
                            if String.isNullOrEmpty r.XElement.Value then
                                None
                            else
                                let v = r.XElement.Value.Trim(' ', '\r', '\n')
                                if String.isNullOrEmpty v then None else Some v)
                        |> Array.toList
                })
            |> Array.toList
        { Streams = streams; Media = media }
