module Broker

open System
open System.Buffers
open System.Text
open System.Threading
open System.Threading.Tasks

open RabbitMQ.Stream.Client

open FSharp.Json

type ConsumerHandler<'a> = 'a -> Consumer -> MessageContext -> Task

type Broker<'a> =
    { System: StreamSystem
      CreateProducer: string -> Task<Producer>
      CreateStream: string -> Task
      DeleteStream: string -> Task
      CreateConsumer: string -> IOffsetType -> ConsumerHandler<'a> -> Task<Consumer> }

type OffsetStrategy =
    | First
    | Last
    | Next
    | Time of int64
    | AbsoluteOffset of uint64

let private newProducer (system: StreamSystem) stream =
    system.CreateProducer(new ProducerConfig(Stream = stream, Reference = "MyProducer"))

let private newConsumer (system: StreamSystem) stream offsetSpec handler =
    system.CreateConsumer(
        new ConsumerConfig(
            Reference = "MyConsumer",
            Stream = stream,
            OffsetSpec = offsetSpec,
            MessageHandler =
                fun cs ctx message ->
                    let content =
                        message.Data.Contents.ToArray()
                        |> Encoding.Default.GetString
                        |> Json.deserialize<'a>

                    handler content cs ctx
        )
    )

// For production use, create a policy to manage MaxLengthBytes and MaxSegmentSizeBytes.
// RabbitMQ stores stream in segmented files.
let private newStream (system: StreamSystem) name =
    system.CreateStream(new StreamSpec(name))

let private deleteStream (system: StreamSystem) = system.DeleteStream

let connect username password virtualhost endpoints =
    let config =
        new StreamSystemConfig(
            UserName = username,
            Password = password,
            VirtualHost = virtualhost,
            Endpoints = endpoints
        )

    task {
        let! system = StreamSystem.Create config

        return
            { System = system
              CreateProducer = newProducer system
              CreateStream = newStream system
              CreateConsumer = newConsumer system
              DeleteStream = deleteStream system }
    }

let send (producer: Producer) publishId record =
    let json = Json.serialize record
    let msg = new Message(Encoding.UTF8.GetBytes(json))
    producer.Send(uint64 publishId, msg)

let offset strategy : IOffsetType =
    match strategy with
    | First -> new OffsetTypeFirst()
    | Next -> new OffsetTypeNext()
    | Last -> new OffsetTypeLast()
    | Time ts -> new OffsetTypeTimestamp(ts)
    | AbsoluteOffset value -> new OffsetTypeOffset(value)
