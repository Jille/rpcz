package rpcz

import (
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/Jille/convreq"
	"github.com/Jille/convreq/respond"
	"github.com/Jille/dfr"
	"google.golang.org/grpc/metadata"
)

var Handler http.Handler = convreq.Wrap(handler)

var tpl = template.Must(template.New("").Parse(`
<html>
	<head>
		<title>RPC samples</title>
		<meta charset="utf-8" />
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
		<span style="float: right">Times are in {{.Timezone}}</span>
		<h1>RPC samples</h1>
		<h2>Choose a method</h2>
		<ul>
{{ range .Methods }}
			<li><a href="#{{.ID}}" onclick="showMethod({{.ID}});">{{.Name}}</a></li>
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
					<td>
						<b>{{if .Inbound}}Caller{{else}}Recipient{{end}}</b>: {{if .Peer}}{{.Peer}}{{else}}?{{end}}
						<b>Deadline</b>: {{.Deadline}}
{{ range $k, $v := .Metadata }}
							<i>{{$k}}</i>: {{$v}}
{{ end }}
					</td>
				</tr>
{{ range .Messages }}
				<tr>
{{ if .DroppedMessages }}
					<td></td>
					<td></td>
					<td><i>{{.DroppedMessages}} message(s) omitted</i></td>
{{ else }}
					<td>{{.Time}}</td>
					<td align="right" title="Time since previous message">({{.TimeFromPrev}})</td>
					<td>{{if .Inbound}}recv{{else}}sent{{end}}: {{.Message}}</td>
{{ end }}
				</tr>
{{ end }}
{{ if .Finished }}
				<tr>
					<td>{{.End}}</td>
					<td align="right" title="Total duration of the RPC">({{.Duration}})</td>
					<td><b>{{.StatusCode}}</b>{{if .StatusMessage}}: {{.StatusMessage}}{{end}}</td>
				</tr>
{{ end }}
				<tr>
					<td colspan="3"><hr></td>
				</tr>
{{ end }}
			</table>
		</div>
{{ end }}
		<script type="text/javascript">
			var el = document.getElementById(unescape(document.location.hash.substring(1)));
			if(el) {
				el.style.display = 'block';
			}
		</script>
	</body>
</html>
`))

type templateData struct {
	Methods  []templateMethod
	Timezone string
}

type templateMethod struct {
	Name  string
	ID    string
	Calls []templateCall
}

type templateCall struct {
	Start         string
	End           string
	Deadline      string
	Duration      string
	Finished      bool
	StatusCode    string
	StatusMessage string
	Inbound       bool
	Peer          string
	Metadata      metadata.MD
	Messages      []templateMessage
}

type templateMessage struct {
	DroppedMessages uint64

	Inbound      bool
	Time         string
	TimeFromPrev string
	Message      string
}

func handler(r *http.Request) convreq.HttpResponse {
	if r.Method != "GET" {
		return respond.MethodNotAllowed("Method Not Allowed")
	}
	now := time.Now()
	var d dfr.D
	defer d.Run(nil)
	data := templateData{}
	data.Timezone = now.Format("MST")
	mtx.Lock()
	unlocker := d.Add(mtx.Unlock)
	data.Methods = make([]templateMethod, 0, len(perMethod))
	for m, cfm := range perMethod {
		meth := templateMethod{
			Name:  m,
			ID:    url.PathEscape(m),
			Calls: make([]templateCall, 0, RetainRPCsPerMethod),
		}
		for i := 0; RetainRPCsPerMethod > i; i++ {
			// Reverse order through a ring buffer.
			c := cfm.calls[(RetainRPCsPerMethod+cfm.ptr-1-i)%RetainRPCsPerMethod]
			if c == nil {
				continue
			}
			firstMessages := c.firstMessages()
			lastMessages := c.lastMessages()
			msgs := make([]templateMessage, len(firstMessages), len(firstMessages)+len(lastMessages)+1)
			prev := c.start
			for j, msg := range c.firstMessages() {
				msgs[j] = templateMessage{
					Inbound:      msg.inbound,
					Time:         timeToString(now, msg.stamp),
					TimeFromPrev: msg.stamp.Sub(prev).String(),
					Message:      msg.message,
				}
				prev = msg.stamp
			}
			if c.droppedMessages() > 0 {
				msgs = append(msgs, templateMessage{
					DroppedMessages: c.droppedMessages(),
				})
			}
			for _, msg := range lastMessages {
				msgs = append(msgs, templateMessage{
					Inbound:      msg.inbound,
					Time:         timeToString(now, msg.stamp),
					TimeFromPrev: msg.stamp.Sub(prev).String(),
					Message:      msg.message,
				})
				prev = msg.stamp
			}
			p := "?"
			if c.peer != nil {
				p = c.peer.String()
			}
			dl := c.deadline.String()
			if c.deadline == 0 {
				dl = "none"
			}
			meth.Calls = append(meth.Calls, templateCall{
				Start:         timeToString(now, c.start),
				Deadline:      dl,
				Finished:      c.duration != 0,
				Duration:      c.duration.String(),
				End:           timeToString(now, c.start.Add(c.duration)),
				StatusCode:    c.statusCode.String(),
				StatusMessage: c.statusMessage,
				Inbound:       c.inbound,
				Peer:          p,
				Metadata:      c.metadata,
				Messages:      msgs,
			})
		}
		data.Methods = append(data.Methods, meth)
	}
	unlocker(true)
	sort.Slice(data.Methods, func(i, j int) bool {
		return data.Methods[i].Name < data.Methods[j].Name
	})
	return respond.RenderTemplate(tpl, data)
}

func timeToString(now, t time.Time) string {
	t = t.Round(0)
	if now.Sub(t) < 12*time.Hour {
		return t.Format("15:04:05.000000000")
	}
	return t.Format("2006-01-02 15:04:05.000000000")
}
