defmodule MyApp do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: MyAppExample,
      producer: [
        module: {MyApp.Counter, 0},
        transformer: {MyApp.CounterMessage, :transform, []},
        stages: 1
      ],
      processors: [
        default: [stages: 2]
      ],
      batchers: [
        default: [stages: 1, batch_size: 5, batch_timeout: 15_000],
        fizzbuzz: [stages: 1, batch_size: 5, batch_timeout: 15_000],
        fizz: [stages: 1, batch_size: 5, batch_timeout: 15_000],
        buzz: [stages: 1, batch_size: 5, batch_timeout: 15_000],
      ]
    )
  end

  def handle_message(:default, %Message{data: data} = message, _context) do
    Process.sleep 250
    result = fizzbuzz(data)

    message
    |> Message.put_batcher(result)
  end

  def handle_batch(:default, messages, _batch_info, _context) do
    data = Enum.map(messages, &(&1.data)) |> Enum.join(", ")
    IO.inspect "Default: #{data}"
    messages
  end

  def handle_batch(:fizz, messages, _batch_info, _context) do
    data = Enum.map(messages, &(&1.data)) |> Enum.join(", ")
    IO.inspect "Fizz: #{data}"
    messages
  end

  def handle_batch(:buzz, messages, _batch_info, _context) do
    data = Enum.map(messages, &(&1.data)) |> Enum.join(", ")
    IO.inspect "Buzz: #{data}"
    messages
  end

  def handle_batch(:fizzbuzz, messages, _batch_info, _context) do
    data = Enum.map(messages, &(&1.data)) |> Enum.join(", ")
    IO.inspect "Fizzbuzz: #{data}"
    messages
  end

  defp fizzbuzz(data), do: _fizzbuzz(data, rem(data, 3), rem(data, 5))
  defp _fizzbuzz(_data, 0, 0), do: :fizzbuzz
  defp _fizzbuzz(_data, 0, _), do: :fizz
  defp _fizzbuzz(_data, _, 0), do: :buzz
  defp _fizzbuzz(_data, _, _), do: :default
end
