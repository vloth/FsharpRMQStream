module Utils

open System.Threading.Tasks

let optionalFloat (f: float) =
    match f with
    | f when System.Double.IsNaN(f) -> None
    | _ -> Some(f)

let whenAll (tasks: seq<ValueTask>) =
    [| for t in tasks -> task { return! t.ConfigureAwait false } |]
    |> Task.WhenAll

let waitAll (tasks: seq<ValueTask>) =
    seq { for t in tasks -> task { return! t.ConfigureAwait false } }
    |> Seq.cast<Task>
    |> Array.ofSeq
    |> Task.WaitAll

let disregard2 f = fun y _ _ -> f y
