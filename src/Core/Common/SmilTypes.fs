namespace Nrk.Oddjob.Core

module SmilTypes =

    open FSharp.Data
    open XmlTypes

    type private SmilXmlProvider =
        XmlProvider<"""
            <smil>
             <body>
              <switch>
               <video src="MDRE30001620CA_0000000000000000072580_ID180.mp4" />
               <video src="MDRE30001620CA_0000000000000000072580_ID270.mp4" />
               <textstream src="MDRE30001620_NOR.vtt" />
               <textstream src="MDRE30001620_NOR.vtt" language="nor" />
               <textstream src="MDRE30001620_NOR.vtt" language="nor" subtitleName = "" />
               <textstream src="MDRE30001620_NOR.vtt" language="nor" subtitleName = "" subtitleAccessibility = "" />
               <textstream src="MDRE30001620_NOR.vtt" language="nor" subtitleName = "" subtitleAccessibility = "" subtitleDefault = "" />
               <textstream src="MDRE30001620_NOR.vtt" language="nor" subtitleName = "" subtitleAccessibility = "" subtitleDefault = "" systemLanguage = "" />
              </switch>
              <switch>
               <video src="MDRE30001620CA_0000000000000000072580_ID180.mp4" />
               <video src="MDRE30001620CA_0000000000000000072580_ID270.mp4" />
              </switch>
              <switch>
               <audio src="DMTS41050019_0_ID64AAC.mp4" />
               <audio src="DMTS41050019_0_ID192AAC.mp4" />
              </switch>
             </body>
            </smil>""">

    type SmilVideo = { Src: string }

    type SmilAudio = { Src: string }

    type SmilTextStream =
        {
            Src: string
            Language: string
            SystemLanguage: string
            SubtitleName: string
            SubtitleAccessibility: string
            SubtitleDefault: string
        }

    type SmilElement =
        | Video of SmilVideo
        | Audio of SmilAudio
        | TextStream of SmilTextStream

    type SmilSwitch = { Switch: SmilElement list }

    type SmilDocument =
        {
            Content: SmilSwitch list
        }

        member this.Serialize() =
            let xml =
                Root
                    [
                        Element(
                            "smil",
                            [],
                            "",
                            [
                                Element(
                                    "body",
                                    [],
                                    "",
                                    this.Content
                                    |> Seq.map (fun switch ->
                                        Element(
                                            "switch",
                                            [],
                                            "",
                                            switch.Switch
                                            |> Seq.map (fun element ->
                                                match element with
                                                | Video video -> Element("video", [ ("src", video.Src) ], "", [])
                                                | Audio audio -> Element("audio", [ ("src", audio.Src) ], "", [])
                                                | TextStream textStream ->
                                                    Element(
                                                        "textstream",
                                                        [
                                                            yield ("src", textStream.Src)
                                                            if String.isNotNullOrEmpty textStream.Language then
                                                                yield ("language", textStream.Language)
                                                            if String.isNotNullOrEmpty textStream.SystemLanguage then
                                                                yield ("systemLanguage", textStream.SystemLanguage)
                                                            if String.isNotNullOrEmpty textStream.SubtitleName then
                                                                yield ("subtitleName", textStream.SubtitleName)
                                                            if String.isNotNullOrEmpty textStream.SubtitleDefault then
                                                                yield ("subtitleDefault", textStream.SubtitleDefault)
                                                            if String.isNotNullOrEmpty textStream.SubtitleAccessibility then
                                                                yield ("subtitleAccessibility", textStream.SubtitleAccessibility)
                                                        ],
                                                        "",
                                                        []
                                                    ))
                                        ))
                                )
                            ]
                        )
                    ]
            xml.ToString()

        static member Deserialize(xml: string) =

            let getNonEmptyValueOrNull s =
                if String.isNullOrEmpty s then null else s

            let smil = SmilXmlProvider.Parse xml
            {
                SmilDocument.Content =
                    smil.Body.Switches
                    |> Seq.map (fun switch ->
                        let elements =
                            [
                                switch.Videos |> Seq.map (fun x -> Video { Src = getNonEmptyValueOrNull x.Src })
                                switch.Audios |> Seq.map (fun x -> Audio { Src = getNonEmptyValueOrNull x.Src })
                                switch.Textstreams
                                |> Seq.map (fun x ->
                                    TextStream
                                        {
                                            Src = getNonEmptyValueOrNull x.Src
                                            Language = Option.toObj x.Language
                                            SystemLanguage = getNonEmptyValueOrNull x.SystemLanguage
                                            SubtitleName = getNonEmptyValueOrNull x.SubtitleName
                                            SubtitleAccessibility = getNonEmptyValueOrNull x.SubtitleAccessibility
                                            SubtitleDefault = getNonEmptyValueOrNull x.SubtitleDefault
                                        })
                            ]
                            |> Seq.concat
                        { Switch = elements |> Seq.toList })
                    |> Seq.toList
            }

        static member Zero = { Content = List.empty }

        member this.GetFileNameBase() =
            (this.Serialize() |> Akka.Util.MurmurHash.StringHash).ToString("x8").Substring(0, 8)
