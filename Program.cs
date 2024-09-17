using System.Xml.Linq;
class Program{
    static void Main(){
        XDocument xml = XDocument.Load("data.xml");
        XElement? orders = xml.Element("orders");

        var orders_linq = xml.Element("orders")?
            .Elements("order")
            .Select(p => new {
                no = p.Element("no")?.Value,
                reg_date = p.Element("reg_date")?.Value,
                sum = p.Element("sum")?.Value,
                products = p.Elements("product")
                    .Select(n => new {
                        quantity = n.Element("quantity")?.Value,
                        name = n.Element("name")?.Value,
                        price = n.Element("price")?.Value
                    })
            });

        if (orders_linq is not null){
            foreach(var order in orders_linq){
                Console.WriteLine($"no = {order.no}\nregdate = {order.reg_date}\nsum = {order.sum}\nproducts = {order.products}");
            }
        }

        if (orders is not null){
            foreach(XElement order in orders.Elements("order")){
                XElement? sum = order.Element("sum");
                Console.WriteLine(sum?.Value);
                XElement? user = order.Element("user");
                XElement? fio = user?.Element("fio");
                XElement? email = user?.Element("email");
                
                foreach(XElement product in order.Elements("product")){


                    UpdateUsers(user);

                }

            }
        }
    }

    static void UpdateUsers(XElement? user){
        XElement? fio = user?.Element("fio");
        XElement? email = user?.Element("fio");
    }

    static void UpdateProducts(XElement? product){
        XElement? quantity = product?.Element("quantity");
        XElement? name = product?.Element("name");
        XElement? price = product?.Element("price");

        Console.WriteLine("Table products was updated");
    }

    static void UpdateOrders(){
        Console.WriteLine("Table orders was updated");
    }

    static void UpdateOrderElement(){
        Console.WriteLine("Table order_element was updated");
    }
}