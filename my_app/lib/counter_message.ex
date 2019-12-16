defmodule MyApp.CounterMessage do
  alias Broadway.{Acknowledger, Message}

  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, event}
    }
    |> validate_even
  end

  def ack(_ref, _successes, _failures) do
    :ok
  end

  defp validate_even(%Message{data: event} = message) when rem(event, 2) == 0 do
    message
  end

  defp validate_even(%Message{} = message) do
    Message.failed(message, :odd)
  end
end
