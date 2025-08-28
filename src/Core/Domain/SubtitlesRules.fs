namespace Nrk.Oddjob.Core

module SubtitlesRules =

    module V1 =
        type SubtitleRuleSet =
            {
                Functions: string list
                IsSingle: bool
                Label: string
                LanguageCode: string
                SubtitleAccessibility: string
                IsDefaultTrack: bool
            }
        let generateRulesOld singleOrSameLangLabel translatedLabel languageCode =
            [
                {
                    IsSingle = true
                    Functions = [ "*" ]
                    Label = singleOrSameLangLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = null
                    IsDefaultTrack = true
                }
                {
                    IsSingle = false
                    Functions = [ "ttv"; "tdhh" ]
                    Label = translatedLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = "transcribes-spoken-dialog"
                    IsDefaultTrack = true
                }
                {
                    IsSingle = false
                    Functions = [ "nor"; "over" ]
                    Label = singleOrSameLangLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = null
                    IsDefaultTrack = false
                }
            ]

        let generateRules langLabel onlyOtherLangLabel languageCode =
            [
                {
                    IsSingle = true
                    Functions = [ "nor"; "over" ]
                    Label = langLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = null
                    IsDefaultTrack = true
                }
                {
                    IsSingle = true
                    Functions = [ "ttv"; "tdhh" ]
                    Label = langLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = "transcribes-spoken-dialog"
                    IsDefaultTrack = true
                }
                {
                    IsSingle = false
                    Functions = [ "ttv"; "tdhh" ]
                    Label = langLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = "transcribes-spoken-dialog"
                    IsDefaultTrack = false
                }
                {
                    IsSingle = false
                    Functions = [ "nor"; "over" ]
                    Label = onlyOtherLangLabel
                    LanguageCode = languageCode
                    SubtitleAccessibility = null
                    IsDefaultTrack = true
                }
            ]

        let generateRulesNonNorwegian label languageCode =
            [
                {
                    IsSingle = true
                    Functions = [ "*" ]
                    Label = label
                    LanguageCode = languageCode
                    SubtitleAccessibility = null
                    IsDefaultTrack = true
                }
                {
                    IsSingle = false
                    Functions = [ "ttv"; "tdhh" ]
                    Label = label
                    LanguageCode = languageCode
                    SubtitleAccessibility = "transcribes-spoken-dialog"
                    IsDefaultTrack = true
                }
            ]

        (*
        List of subtitle groups
        string list * SubtitleRuleSet list
        can be moved to a config file if all language groups follow same ruleset, only labels differ
        
        eg. Norsk includes language codes "no", "nb" and "nn"
        maps to rules for this language group "Norsk" and "Norsk - på all tale" 
        for distinct subtitle functions depending on number of subtitles
        
        Internally in a group use first rule that matches all rules
        
        Subtitles are ordered in same order as in this config
        internally in a subtitle group follows order of subtitleRuleSet list
        *)
        let subtitlesConfig =
            [
                yield ([ "no"; "nb"; "nn"; "smi" ], generateRulesOld "Norsk" "Norsk - på all tale" "nor")
                yield ([ "sme"; "se" ], generateRules "Davvisámegillii" "Davvisámegillii – go eará giella gullo" "sme")
                yield ([ "smj" ], generateRules "Julevsábmáj" "Julevsábmáj – gå ietjá giella gullu" "smj")
                yield ([ "sma" ], generateRules "Åarjelsaemien" "Åarjelsaemien – ajve gosse jeatjah gïele govloe" "sma")
                yield ([ "fkv" ], generateRulesNonNorwegian "Kvääni" "fkv")
            ]

    module V2 =
        type SubtitleRuleSet =
            {
                Name: string
                Label: string
                LanguageCode: string
                SubtitleAccessibilityRoleRule: (string * string) option // if has fst role, set snd subtitlesccesability
                IsDefaultTrack: bool // this overrides previously included tracks, and sets them to false
            }

        let private fulltext label languageCode =
            {
                Name = "non-sdh-fulltext"
                Label = label
                LanguageCode = languageCode
                SubtitleAccessibilityRoleRule = ("x-nrk-no-same-language", "transcribes-spoken-dialog") |> Some
                IsDefaultTrack = true
            }

        let private translated label languageCode =
            {
                Name = "non-sdh-translated"
                Label = label
                LanguageCode = languageCode
                SubtitleAccessibilityRoleRule = None
                IsDefaultTrack = false
            }

        let private sdh label languageCode =
            {
                Name = "sdh"
                Label = label
                LanguageCode = languageCode
                SubtitleAccessibilityRoleRule = ("sound", "describes-music-and-sound") |> Some
                IsDefaultTrack = false
            }

        let subtitlesConfig =
            [
                ([ "no"; "nb"; "nn"; "smi" ], fulltext "Norsk" "nor")
                ([ "no"; "nb"; "nn"; "smi" ], translated "Norsk – kun ved annet språk" "nor")
                ([ "no"; "nb"; "nn"; "smi" ], sdh "Norsk – med lydbeskrivelser" "nor")

                ([ "sme"; "se" ], fulltext "Davvisámegillii" "sme")
                ([ "sme"; "se" ], translated "Davvisámegillii – go eará giella gullo" "sme")
                ([ "sme"; "se" ], sdh "Davvisámegillii – jienat čilgejuvvojit" "sme")

                ([ "sma" ], fulltext "Åarjelsaemien" "sma")
                ([ "sma" ], translated "Åarjelsaemien – ajve gosse jeatjah gïele govloe" "sma")
                ([ "sma" ], sdh "Åarjelsaemien – tjoejebuerkehtsigujmie" "sma")

                ([ "smj" ], fulltext "Julevsábmáj" "smj")
                ([ "smj" ], translated "Julevsábmáj – gå ietjá giella gullu" "smj")
                ([ "smj" ], sdh "Julevsábmáj – jiednatjielggidusá" "smj")

                ([ "fkv" ], fulltext "Kvääni" "fkv")
            ]
