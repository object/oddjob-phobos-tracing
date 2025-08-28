namespace Nrk.Oddjob.Core

// http://trelford.com/blog/post/F-XML-Comparison-(XElement-vs-XmlDocument-vs-XmlReaderXmlWriter-vs-Discriminated-Unions).aspx
module XmlTypes =

    open System.IO
    open System.Text
    open System.Xml

    type StringWriterWithEncoding(builder: StringBuilder, encoding: Encoding) =
        inherit StringWriter(builder)
        override this.Encoding = encoding

    type Attribute = string * string

    type Xml =
        | Root of Xml seq
        | Element of name: string * attributes: Attribute seq * value: string * Xml seq

        member this.Serialize(writer: XmlWriter) =
            let rec Write element =
                match element with
                | Root children ->
                    writer.WriteStartDocument()
                    children |> Seq.iter Write
                    writer.WriteEndDocument()
                | Element(name, attributes, value, children) ->
                    writer.WriteStartElement(name)
                    attributes |> Seq.iter (fun (name, value) -> writer.WriteAttributeString(name, "", value))
                    writer.WriteString(value)
                    children |> Seq.iter Write
                    writer.WriteEndElement()
            Write this

        override this.ToString() =
            let output = StringBuilder()
            let settings = XmlWriterSettings()
            settings.Indent <- true
            use writer = new XmlTextWriter(new StringWriterWithEncoding(output, Encoding.UTF8))
            writer.Formatting <- Formatting.Indented
            this.Serialize writer
            output.ToString()
