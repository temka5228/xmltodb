using System.Xml.Linq;
using Npgsql;
using System.Globalization;
using System.Numerics;
using System.Threading.Tasks;
using System.Linq;

class Program
{
    static async Task Main(string[] args)
    {
        var processor = new OrderProcessor("Host=localhost;Port=5432;Database=istore;User Id=postgres;Password=1488;");
        await processor.ProcessOrdersAsync("data.xml");
    }
}

// OrderProcessor class handles the main logics
public class OrderProcessor
{
    private readonly string _connectionString;

    public OrderProcessor(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task ProcessOrdersAsync(string xmlFilePath)
    {
        var xml = XDocument.Load(xmlFilePath);

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var orders = ParseOrders(xml);

        using var transaction = await connection.BeginTransactionAsync();
        try
        {
            foreach (var order in orders)
            {
                await InsertDB(connection, "users", ["fio", "email"], [order?.User?.Fio, order?.User?.Email]);
                var userId = await GetFirstIdAsync(connection, "users", "email", order?.User?.Email);
                await InsertDB(connection, "orders", ["id", "user_id", "total_amount", "date"], [order?.No, userId, order?.Sum, order?.RegDate]);
                
                foreach (var product in order.Products)
                {
                    await InsertDB(connection, "products", ["name", "price"], [product?.Name, product?.Price]);
                    var productId = await GetFirstIdAsync(connection, "products", "name", product?.Name);
                    await InsertDB(connection, "order_element", ["product_id", "order_id", "quantity"], [productId, order?.No, product?.Quantity]);
                }
            }
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
        }
    }

    private IEnumerable<Order>? ParseOrders(XDocument xml)
    {
        return xml.Element("orders")?
            .Elements("order")?
            .Select(p => new Order
            {
                No = int.Parse(p.Element("no")?.Value),
                RegDate = DateOnly.FromDateTime(DateTime.ParseExact(p.Element("reg_date")?.Value, "yyyy.MM.dd", CultureInfo.InvariantCulture)),
                Sum = double.Parse(p.Element("sum")?.Value, CultureInfo.InvariantCulture),
                User = new User
                {
                    Fio = p.Element("user")?.Element("fio")?.Value,
                    Email = p.Element("user")?.Element("email")?.Value
                },
                Products = p.Elements("product")?
                    .Select(n => new Product
                    {
                        Quantity = int.Parse(n.Element("quantity")?.Value),
                        Name = n.Element("name")?.Value,
                        Price = double.Parse(n.Element("price")?.Value, CultureInfo.InvariantCulture)
                    }).ToList()
            }).ToList();
    }

    private async Task InsertDB(NpgsqlConnection con, string? table, string?[] colNames, object?[] columns)
    {
        NpgsqlCommand command = new NpgsqlCommand();
        string strColNames = string.Join(", ", colNames);
        string[] ParColNames = colNames.Select(p => "@" + p).ToArray();
        string strSepColNames = string.Join(", ", ParColNames);
        string[] andColNames = colNames.Select(p => p + " = @" + p).ToArray();
        string strAndColNames = string.Join(" and ", andColNames);
        string prepareToCommand = 
            $"insert into {table} ({strColNames}) "
            + $"select {strSepColNames} "
            + "where not exists ("
            + $"select 1 from {table} "
            + $"where {strAndColNames});";
        command.CommandText = string.Format(prepareToCommand);
        var zipCol = ParColNames.Zip(columns);
        for (int i = 0; i < colNames.Length; i++){
            command.Parameters.AddWithValue(colNames[i].ToString(), columns[i]);
        }
        command.Connection = con;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int> GetFirstIdAsync(NpgsqlConnection con, string? table, string? column, string? value)
    {
        NpgsqlCommand command = new NpgsqlCommand();
        command.Connection = con;
        command.CommandText = $"select id from {table} where {column} = @value;";
        command.Parameters.AddWithValue("value", value);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }
}

// Data models
public class Order
{
    public int? No { get; set; }
    public DateOnly? RegDate { get; set; }
    public double? Sum { get; set; }
    public User? User { get; set; }
    public List<Product>? Products { get; set; }
}

public class User
{
    public string? Fio { get; set; }
    public string? Email { get; set; }
}

public class Product
{
    public int? Quantity { get; set; }
    public string? Name { get; set; }
    public double? Price { get; set; }
}
