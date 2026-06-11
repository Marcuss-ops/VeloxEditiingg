package explorer

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

// APIExplorer returns an HTML interface showing the current routing tree.
func APIExplorer(r *gin.Engine) gin.HandlerFunc {
	// Simple map to store routes grouped by prefix
	return func(c *gin.Context) {
		routes := r.Routes()

		// Group routes by semantic category
		grouped := make(map[string][]gin.RouteInfo)
		for _, route := range routes {
			path := route.Path
			var category string

			if strings.Contains(path, "ansible") {
				category = "[TOOL] Ansible (Gestione Server)"
			} else if strings.Contains(path, "finance") {
				category = "[NAV] Finance & Revenue"
			} else if strings.Contains(path, "/summary") || strings.Contains(path, "/realtime") {
				category = "[NAV] Panoramica & Dashboard Principale"
			} else if strings.Contains(path, "dashboard") || strings.Contains(path, "analytics") {
				category = "[NAV] Views & Metriche"
			} else if strings.Contains(path, "youtube") || strings.Contains(path, "channel") {
				category = "[PLAY] YouTube Manager"
			} else if strings.HasPrefix(path, "/api/v1/jobs") || path == "/jobs" || strings.HasPrefix(path, "/api/v1/queue") || strings.Contains(path, "job") {
				category = "[CONFIG] Generazione & Jobs"
			} else if strings.Contains(path, "worker") {
				category = "[NAV] Gestione Worker"
			} else if strings.Contains(path, "drive") || strings.Contains(path, "clip") || strings.Contains(path, "stock") {
				category = "[NAV] Cloud Storage & Assets"
			} else if strings.Contains(path, "script") || strings.Contains(path, "pipeline") {
				category = "[NAV] Scripting & Pipeline AI"
			} else if strings.Contains(path, "health") || strings.Contains(path, "status") || strings.Contains(path, "explorer") {
				category = "[NAV] Monitoraggio & API System"
			} else {
				category = "[NAV] Altri Endpoint API"
			}

			grouped[category] = append(grouped[category], route)
		}

		// HTML template styling (Tailwind CSS via CDN)
		html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Velox API Explorer</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        body { background-color: #0f172a; color: #e2e8f0; font-family: 'Inter', sans-serif; }
        .method-GET { background-color: #0ea5e9; color: white; }
        .method-POST { background-color: #10b981; color: white; }
        .method-DELETE { background-color: #ef4444; color: white; }
        .method-PUT { background-color: #f59e0b; color: white; }
        .method-PATCH { background-color: #8b5cf6; color: white; }
        .method-OPTIONS, .method-HEAD, .method-ANY { background-color: #64748b; color: white; }
    </style>
</head>
<body class="p-8">
    <div class="max-w-6xl mx-auto">
        <div class="flex items-center space-x-4 mb-8 border-b border-slate-700 pb-4">
            <i class="fas fa-network-wired text-4xl text-emerald-400"></i>
            <div>
                <h1 class="text-3xl font-bold text-white">Velox API Explorer</h1>
                <p class="text-slate-400 mt-1">Live routing tree directly from Go Backend</p>
            </div>
        </div>

        <div class="grid gap-6">`

		// Sort keys
		var keys []string
		for k := range grouped {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, groupPrefix := range keys {
			groupRoutes := grouped[groupPrefix]

			html += `<div class="bg-slate-800 rounded-lg shadow-lg border border-slate-700 overflow-hidden">
                <div class="bg-slate-900 px-4 py-3 border-b border-slate-700 flex justify-between items-center cursor-pointer" onclick="this.nextElementSibling.classList.toggle('hidden')">
                    <h2 class="text-xl font-semibold text-slate-200">` + groupPrefix + `</h2>
                    <span class="bg-slate-700 text-xs px-2 py-1 rounded text-slate-300">` + string(rune(len(groupRoutes)+48)) + ` endpoints</span>
                </div>
                <div class="p-4 space-y-3 block">` // Removed hidden for visibility

			for _, route := range groupRoutes {
				methodClass := "method-" + route.Method
				if route.Method == "ANY" || route.Method == "" {
					methodClass = "method-ANY"
				}

				html += `<div class="flex items-center bg-slate-800/50 hover:bg-slate-700/50 border border-slate-700 p-2 rounded transition-colors group">
                        <span class="` + methodClass + ` font-mono text-xs font-bold px-2 py-1 rounded w-20 text-center mr-4">` + route.Method + `</span>
                        <span class="font-mono text-emerald-300 text-sm flex-grow">` + route.Path + `</span>
                        <span class="font-mono text-xs text-slate-500 hidden group-hover:block">` + route.Handler + `</span>
                        <a href="` + route.Path + `" target="_blank" class="text-slate-500 hover:text-emerald-400 ml-4"><i class="fas fa-external-link-alt"></i></a>
                    </div>`
			}

			html += `</div></div>`
		}

		html += `
        </div>
        <div class="mt-8 text-center text-slate-500 text-sm">
            Powered by Velox Go Gateway & Gin Web Framework
        </div>
    </div>
</body>
</html>`

		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
	}
}

// APIExplorerJSON endpoints for purely programmatic access
func APIExplorerJSON(r *gin.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		routes := r.Routes()
		c.JSON(http.StatusOK, gin.H{
			"ok":     true,
			"total":  len(routes),
			"routes": routes,
		})
	}
}
