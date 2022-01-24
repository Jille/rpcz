package rpcz

import (
	"fmt"
	"html/template"
	"net/http"
	"sort"

	"github.com/Jille/convreq"
	"github.com/Jille/convreq/respond"
)

var Handler http.Handler = convreq.Wrap(handler)

var tpl = template.Must(template.New("").Parse(`
<html>
	<head>
		<title>RPC samples</title>
		<script type="text/javascript">
			function showMethod(method) {
				var slides = document.getElementsByClassName('methodBlock');
				for(var i = 0; i < slides.length; i++) {
					slides.item(i).style.display = 'none';
				}
				document.getElementById(method).style.display = 'block';
			}
		</script>
	</head>
	<body>
		<h1>RPC samples</h1>
		<h2>Choose a method</h2>
		<ul>
{{ range .Methods }}
			<li><a href="#" onclick="showMethod({{.ID}}); return false;">{{.Name}}</a></li>
{{ end }}
		</ul>
{{ range .Methods }}
		<div id="{{.ID}}" class="methodBlock" style="display: none">
			<h2>{{.Name}}</h2>
			<table>
{{ range .Calls }}
				<tr>
					<td>{{.Start}}</td>
					<td></td>
					<td><b>Peer</b>: {{if .Peer}}{{.Peer}}{{else}}?{{end}}  <b>Deadline</b>: {{.Deadline}}</td>
				</tr>
{{ range .Messages }}
				<tr>
					<td>{{.Time}}</td>
					<td>({{.TimeFromPrev}})</td>
					<td>{{if .Inbound}}recv{{else}}sent{{end}}: {{.Message}}</td>
				</tr>
{{ end }}
{{ if .Duration }}
				<tr>
					<td>{{.End}}</td>
					<td>({{.Duration}})</td>
					<td><b>Status</b>: {{.Status}}</td>
				</tr>
{{ end }}
				<tr>
					<td colspan="3"><hr></td>
				</tr>
{{ end }}
			</table>
		</div>
{{ end }}
	</body>
</html>
`))

type templateData struct {
	Methods []templateMethod
}

type templateMethod struct {
	Name  string
	ID    string
	Calls []templateCall
}

type templateCall struct {
	Start    string
	End      string
	Deadline string
	Duration string
	Status   string
	Peer     string
	Messages []templateMessage
}

type templateMessage struct {
	Inbound      bool
	Time         string
	TimeFromPrev string
	Message      string
}

func handler(r *http.Request) convreq.HttpResponse {
	if r.Method != "GET" {
		return respond.MethodNotAllowed("Method Not Allowed")
	}
	data := templateData{}
	mtx.Lock()
	data.Methods = make([]templateMethod, 0, len(perMethod))
	for m, cfm := range perMethod {
		meth := templateMethod{
			Name:  m,
			ID:    fmt.Sprintf("mth%d", len(data.Methods)),
			Calls: make([]templateCall, 0, RetainRPCsPerMethod),
		}
		for i := 0; RetainRPCsPerMethod > i; i++ {
			c := cfm.calls[(cfm.ptr+i)%RetainRPCsPerMethod]
			if c == nil {
				continue
			}
			msgs := make([]templateMessage, len(c.messages))
			prev := c.start
			for j, msg := range c.messages {
				msgs[j] = templateMessage{
					Inbound:      msg.inbound,
					Time:         msg.stamp.Round(0).String(),
					TimeFromPrev: msg.stamp.Sub(prev).Round(0).String(),
					Message:      msg.message,
				}
				prev = msg.stamp
			}
			p := "?"
			if c.peer != nil {
				p = c.peer.String()
			}
			meth.Calls = append(meth.Calls, templateCall{
				Start:    c.start.Round(0).String(),
				Deadline: c.deadline.Round(0).String(),
				Duration: c.duration.String(),
				End:      c.start.Add(c.duration).Round(0).String(),
				Status:   c.status.String(),
				Peer:     p,
				Messages: msgs,
			})
		}
		data.Methods = append(data.Methods, meth)
	}
	mtx.Unlock()
	sort.Slice(data.Methods, func(i, j int) bool {
		return data.Methods[i].Name < data.Methods[j].Name
	})
	return respond.RenderTemplate(tpl, data)
}
