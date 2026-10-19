using System.IO.Pipes;
using System.Text;

namespace Client;

class Program
{
    const string PipeName = "StudentRequestPipe";

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;
        string clientName = args.Length > 0 ? args[0] : "Клиент";

        try
        {
            using var pipe = new NamedPipeClientStream(".", PipeName,
                PipeDirection.InOut, PipeOptions.Asynchronous);
            await pipe.ConnectAsync(5000);

            using var reader = new StreamReader(pipe, Encoding.UTF8, false, 1024, true);
            using var writer = new StreamWriter(pipe, new UTF8Encoding(false), 1024, true)
            {
                AutoFlush = true
            };

            var numbers = new string[8];
            for (int i = 0; i < numbers.Length; i++)
                numbers[i] = Guid.NewGuid().ToString("N");

            Console.WriteLine($"{clientName}: подключение установлено");
            Task responses = ReceiveResponsesAsync(reader, numbers, clientName);

            for (int i = 0; i < numbers.Length; i++)
            {
                await writer.WriteLineAsync(numbers[i]);
                Console.WriteLine($"{clientName}: отправлена заявка {i + 1}/8, номер {numbers[i]}");
            }

            Console.WriteLine($"{clientName}: все 8 заявок отправлены");
            await responses;
            Console.WriteLine($"{clientName}: получены все 8 ответов");
        }
        catch (TimeoutException)
        {
            Console.Error.WriteLine($"{clientName}: сервер недоступен, сначала запустите сервер");
            Environment.ExitCode = 1;
        }
        catch (IOException e)
        {
            Console.Error.WriteLine($"{clientName}: ошибка обмена: {e.Message}");
            Environment.ExitCode = 1;
        }
    }

    static async Task ReceiveResponsesAsync(StreamReader reader, string[] numbers,
        string clientName)
    {
        var pending = new HashSet<string>(numbers);

        while (pending.Count > 0)
        {
            string? response = await reader.ReadLineAsync();
            if (response == null)
                throw new IOException("Сервер закрыл канал до получения всех ответов");

            string[] parts = response.Split('|', 2);
            if (parts.Length != 2 || !pending.Remove(parts[0]))
                throw new IOException("Получен неверный ответ или неизвестный номер заявки");

            Console.WriteLine($"{clientName}: ответ на заявку {parts[0]}: {parts[1]}");
        }
    }
}
