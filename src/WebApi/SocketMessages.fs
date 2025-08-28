module SocketMessages // Client expect module-qualified message name, the module name should match the client-side definition

open Nrk.Oddjob.Core.PubSub

type ClientCommand = | Connect

type ServerEvent =
    | MediaSetStatusEvent of MediaSetStatusUpdate
    | MediaSetResourceEvent of MediaSetRemoteResourceUpdate
    | PriorityQueueEvent of PriorityQueueEvent
    | IngesterCommandEvent of IngesterCommandStatus

type ServerMsg =
    | ClientCommand of ClientCommand
    | ServerEvent of ServerEvent
    | Closed
