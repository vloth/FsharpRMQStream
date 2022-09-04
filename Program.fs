open System
open System.Text
open System.Net
open System.Threading.Tasks
open System.Buffers

open RabbitMQ.Stream.Client

open CO2Data

open Broker

[<Literal>]
let stream = "CO2-emission-stream"

let producer broker =
    task {
        let! producer = broker.CreateProducer stream

        getCO2Entries ()
        |> Seq.mapi (send producer)
        |> Utils.waitAll

        return producer
    }

let consumer broker =
    let consume (c: CO2Emission) =
        if Option.isNone c.Year2018 then
            Console.WriteLine($"{c.CountryName} missing 2018 ${c.Year2018}")
        else
            Console.WriteLine($"{c.CountryName} PRESENT")

        Task.CompletedTask

    let offset = offset First
    let handler = Utils.disregard2 consume

    broker.CreateConsumer stream offset handler


let core =
    task {
        let ip = [| new IPEndPoint(IPAddress.Parse "127.0.0.1", 5552) :> EndPoint |]
        let! broker = connect "guest" "guest" "/" ip

        do! broker.CreateStream stream
        let! producer = producer broker
        let! consumer = consumer broker

        Console.WriteLine("Press any key to stop")
        Console.ReadLine() |> ignore

        let! _ = producer.Close()
        let! _ = consumer.Close()
        do! broker.DeleteStream stream
        let! _ = broker.System.Close()

        return Task.CompletedTask
    }

[<EntryPoint>]
let main args =
    core.Wait()
    0
