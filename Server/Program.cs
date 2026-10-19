using System.IO.Pipes;
using System.Text;

namespace Server;

class Program
{
    const string PipeName = "StudentRequestPipe";
    static readonly SemaphoreSlim processingLimit = new(2, 2);
    static readonly object logLock = new();
    static int activeRequests;

    static async Task Main()
    {
        Console.OutputEncoding = Encoding.UTF8;
        using var stopping = new CancellationTokenSource();
        var clients = new List<Task>();
        int clientCount = 0;

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            if (!stopping.IsCancellationRequested)
            {
                stopping.Cancel();
                WriteLog("Остановка сервера, ожидаем завершения принятых заявок");
            }
        };

        WriteLog("Сервер запущен, для завершения нажмите Ctrl+C");

        while (!stopping.IsCancellationRequested)
        {
            var pipe = new NamedPipeServerStream(PipeName, PipeDirection.InOut,
                NamedPipeServerStream.MaxAllowedServerInstances,
                PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

            try
            {
                await pipe.WaitForConnectionAsync(stopping.Token);
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
                pipe.Dispose();
                break;
            }
            catch (IOException e)
            {
                pipe.Dispose();
                WriteLog($"Ошибка подключения: {e.Message}");
                stopping.Cancel();
                break;
            }

            if (stopping.IsCancellationRequested)
            {
                pipe.Dispose();
                break;
            }

            clientCount++;
            clients.Add(HandleClientAsync(pipe, clientCount, stopping.Token));
        }

        await Task.WhenAll(clients);
        processingLimit.Dispose();
        WriteLog("Все принятые заявки завершены, сервер остановлен");
    }

    static async Task HandleClientAsync(NamedPipeServerStream pipe, int clientNumber,
        CancellationToken stopping)
    {
        using var connection = pipe;
        using var reader = new StreamReader(pipe, Encoding.UTF8, false, 1024, true);
        using var writer = new StreamWriter(pipe, new UTF8Encoding(false), 1024, true)
        {
            AutoFlush = true
        };
        using var responseLock = new SemaphoreSlim(1, 1);
        var requests = new List<Task>();

        WriteLog($"Клиент {clientNumber} подключён");

        try
        {
            while (!stopping.IsCancellationRequested)
            {
                string? number = await reader.ReadLineAsync(stopping);
                if (number == null)
                    break;

                if (!Guid.TryParseExact(number, "N", out _))
                {
                    WriteLog($"Клиент {clientNumber} передал неверный номер заявки");
                    continue;
                }

                requests.Add(ProcessRequestAsync(number, writer, responseLock));
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
        }
        catch (IOException e)
        {
            WriteLog($"Ошибка чтения от клиента {clientNumber}: {e.Message}");
        }

        await Task.WhenAll(requests);
        WriteLog($"Клиент {clientNumber} отключён");
    }

    static async Task ProcessRequestAsync(string number, StreamWriter writer,
        SemaphoreSlim responseLock)
    {
        await processingLimit.WaitAsync();
        int delay = Random.Shared.Next(500, 1501);

        lock (logLock)
        {
            activeRequests++;
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Начало заявки {number}, в обработке: {activeRequests}");
        }

        try
        {
            await Task.Delay(delay);
        }
        finally
        {
            lock (logLock)
            {
                activeRequests--;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Конец заявки {number}, в обработке: {activeRequests}");
            }

            processingLimit.Release();
        }

        await responseLock.WaitAsync();
        try
        {
            await writer.WriteLineAsync($"{number}|Обработана за {delay} мс");
        }
        catch (IOException e)
        {
            WriteLog($"Не удалось отправить ответ на заявку {number}: {e.Message}");
        }
        finally
        {
            responseLock.Release();
        }
    }

    static void WriteLog(string message)
    {
        lock (logLock)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
        }
    }
}
