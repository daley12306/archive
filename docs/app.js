var API_URL = "https://api.github.com/repos/daley12306/archive/git/trees/master?recursive=1";
var RAW_BASE = "https://raw.githubusercontent.com/daley12306/archive/master/";

var allFiles = [];

function escapeHtml(text) {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function xhrGet(url, callback) {
  var xhr = new XMLHttpRequest();
  xhr.open("GET", url, true);

  xhr.onreadystatechange = function () {
    if (xhr.readyState === 4) {
      if (xhr.status === 200 || xhr.status === 0) {
        callback(null, xhr.responseText);
      } else {
        callback("HTTP " + xhr.status, null);
      }
    }
  };

  xhr.send(null);
}

function isViewableFile(path) {
  var lower = path.toLowerCase();

  if (lower.indexOf(".png") > -1) return false;
  if (lower.indexOf(".jpg") > -1) return false;
  if (lower.indexOf(".jpeg") > -1) return false;
  if (lower.indexOf(".gif") > -1) return false;
  if (lower.indexOf(".webp") > -1) return false;
  if (lower.indexOf(".zip") > -1) return false;
  if (lower.indexOf(".ipa") > -1) return false;
  if (lower.indexOf(".pdf") > -1) return false;

  return true;
}

function renderLines(text) {
  var lines = escapeHtml(text).split("\n");
  var html = "";

  for (var i = 0; i < lines.length; i++) {
    html += '<span class="line"><span class="num">' +
      (i + 1) +
      "</span>" +
      lines[i] +
      "</span>";
  }

  return html;
}

function renderNotebook(text) {
  var nb;
  var html = "";

  try {
    nb = JSON.parse(text);
  } catch (e) {
    return renderLines(text);
  }

  if (!nb.cells) {
    return renderLines(text);
  }

  for (var i = 0; i < nb.cells.length; i++) {
    var cell = nb.cells[i];
    var source = "";

    if (cell.source) {
      if (typeof cell.source === "string") {
        source = cell.source;
      } else {
        source = cell.source.join("");
      }
    }

    if (cell.cell_type === "markdown") {
      html += '<div class="nb-cell nb-md">';
      html += '<div class="nb-label">Markdown</div>';
      html += '<pre>' + escapeHtml(source) + '</pre>';
      html += '</div>';
    } else if (cell.cell_type === "code") {
      html += '<div class="nb-cell nb-code">';
      html += '<div class="nb-label">Code</div>';
      html += '<pre>' + renderLines(source) + '</pre>';
      html += '</div>';
    }
  }

  return html;
}

function loadRepoFiles() {
  xhrGet(API_URL, function (err, text) {
    var filename = document.getElementById("filename");

    if (err) {
      filename.innerHTML = "Cannot load GitHub API";
      document.getElementById("code").innerHTML =
        "GitHub API lỗi hoặc Safari iOS 6 không hỗ trợ TLS/API.";
      return;
    }

    var data = JSON.parse(text);
    var tree = data.tree;
    allFiles = [];

    for (var i = 0; i < tree.length; i++) {
      if (tree[i].type === "blob" && isViewableFile(tree[i].path)) {
        allFiles.push(tree[i].path);
      }
    }

    renderFileList(allFiles);
    filename.innerHTML = "Select a file";
  });
}

function renderFileList(files) {
  var list = document.getElementById("fileList");
  list.innerHTML = "";

  for (var i = 0; i < files.length; i++) {
    var li = document.createElement("li");
    li.innerHTML = "📄 " + files[i];

    li.onclick = (function (path) {
      return function () {
        loadFile(path);
      };
    })(files[i]);

    list.appendChild(li);
  }
}

function loadFile(path) {
  var filename = document.getElementById("filename");
  var code = document.getElementById("code");

  filename.innerHTML = path;
  code.innerHTML = "Loading...";

  xhrGet(RAW_BASE + path, function (err, text) {
    if (err) {
      code.innerHTML = "Cannot load file: " + path;
      return;
    }

    if (path.toLowerCase().indexOf(".ipynb") > -1) {
      code.innerHTML = renderNotebook(text);
    } else {
      code.innerHTML = renderLines(text);
    }
  });
}

function setupSearch() {
  var searchBox = document.getElementById("searchBox");

  searchBox.onkeyup = function () {
    var keyword = searchBox.value.toLowerCase();
    var result = [];

    for (var i = 0; i < allFiles.length; i++) {
      if (allFiles[i].toLowerCase().indexOf(keyword) !== -1) {
        result.push(allFiles[i]);
      }
    }

    renderFileList(result);
  };
}

function setupSidebarToggle() {
  var toggleBtn = document.getElementById("toggleBtn");

  if (!toggleBtn) return;

  toggleBtn.onclick = function () {
    if (document.body.className.indexOf("sidebar-hidden") >= 0) {
      document.body.className = "";
    } else {
      document.body.className = "sidebar-hidden";
    }
  };
}

setupSearch();
setupSidebarToggle();
loadRepoFiles();
