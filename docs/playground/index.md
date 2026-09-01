# Playground

This page runs tapio in your browser. There is no server. When you press
**Run**, the page downloads a Python interpreter compiled to WebAssembly
([Pyodide](https://pyodide.org)), installs the same `tapio` wheel that ships to
PyPI, and executes the example on the browser's own event loop. The actor
scheduling you watch is real: the interpreter is genuinely running the library,
not replaying a recording.

Pick an example, read it, change it, and run it again. Every example here is
one of the modules in `examples/`, the same code the test suite asserts on, so
what you run is what the documentation describes.

<div id="tapio-pg" class="tapio-pg">
  <div class="tapio-pg__bar">
    <label class="tapio-pg__pick">Example
      <select id="tapio-pg-example" aria-label="Choose an example"></select>
    </label>
    <span class="tapio-pg__spacer"></span>
    <button id="tapio-pg-reset" type="button" class="tapio-pg__btn tapio-pg__btn--ghost">Reset</button>
    <button id="tapio-pg-run" type="button" class="tapio-pg__btn" disabled>Run</button>
  </div>
  <textarea id="tapio-pg-code" class="tapio-pg__code" spellcheck="false" autocapitalize="off" autocomplete="off" autocorrect="off" wrap="off" aria-label="Editable example source"></textarea>
  <div id="tapio-pg-status" class="tapio-pg__status" role="status" aria-live="polite">Loading the examples…</div>
  <pre id="tapio-pg-out" class="tapio-pg__out"><span class="tapio-pg__hint">Output appears here after you run an example.</span></pre>
</div>

!!! note "What runs here, and what does not"

    Everything on this page is a local-only example: it spawns actors, sends
    messages, supervises, watches, and shuts down, all on one event loop. That
    is the half of tapio a browser can run.

    The browser has no sockets and no thread pool, so three groups of examples
    stay in the written documentation rather than here. **Remoting and
    clustering** (two nodes over a TCP link, a cluster gossiping its
    membership, a partition being downed) need real sockets: see
    [Remoting](../remoting.md) and [Clustering](../clustering.md). **Blocking
    offload** (`run_blocking`) needs threads the runtime does not offer in the
    browser: see [Blocking calls](../blocking.md). The `ActorSystem` behind a
    web framework is shown under [Getting started](../getting-started.md).

The first run downloads the interpreter and the wheel, which is a few
megabytes, so it takes a moment. After that, every run is instant, because
nothing is fetched again.

<style>
.tapio-pg {
  border: 1px solid var(--md-default-fg-color--lightest);
  border-radius: .3rem;
  overflow: hidden;
  margin: 1.2rem 0;
}
.tapio-pg__bar {
  display: flex;
  align-items: center;
  gap: .6rem;
  padding: .5rem .7rem;
  background: var(--md-default-fg-color--lightest);
  flex-wrap: wrap;
}
.tapio-pg__pick {
  display: flex;
  align-items: center;
  gap: .4rem;
  font-size: .72rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: .05em;
  color: var(--md-default-fg-color--light);
}
.tapio-pg__pick select {
  font: inherit;
  font-weight: 400;
  text-transform: none;
  letter-spacing: normal;
  padding: .25rem .4rem;
  border-radius: .2rem;
  border: 1px solid var(--md-default-fg-color--lighter);
  background: var(--md-default-bg-color);
  color: var(--md-default-fg-color);
}
.tapio-pg__spacer { flex: 1 1 auto; }
.tapio-pg__btn {
  font: inherit;
  font-weight: 700;
  cursor: pointer;
  padding: .35rem 1rem;
  border: none;
  border-radius: .2rem;
  background: var(--md-primary-fg-color);
  color: var(--md-primary-bg-color);
}
.tapio-pg__btn:disabled { opacity: .5; cursor: progress; }
.tapio-pg__btn--ghost {
  background: transparent;
  color: var(--md-default-fg-color--light);
  border: 1px solid var(--md-default-fg-color--lighter);
}
.tapio-pg__code {
  display: block;
  width: 100%;
  box-sizing: border-box;
  min-height: 20rem;
  resize: vertical;
  border: none;
  border-top: 1px solid var(--md-default-fg-color--lightest);
  padding: .8rem 1rem;
  font-family: var(--md-code-font-family, monospace);
  font-size: .72rem;
  line-height: 1.5;
  tab-size: 4;
  white-space: pre;
  background: var(--md-code-bg-color);
  color: var(--md-code-fg-color);
}
.tapio-pg__code:focus { outline: 2px solid var(--md-primary-fg-color); outline-offset: -2px; }
.tapio-pg__status {
  padding: .4rem 1rem;
  font-size: .7rem;
  color: var(--md-default-fg-color--light);
  background: var(--md-default-fg-color--lightest);
  border-top: 1px solid var(--md-default-fg-color--lightest);
}
.tapio-pg__status[data-kind="error"] { color: #d32f2f; font-weight: 700; }
.tapio-pg__out {
  margin: 0;
  padding: .8rem 1rem;
  min-height: 4rem;
  max-height: 26rem;
  overflow: auto;
  font-family: var(--md-code-font-family, monospace);
  font-size: .72rem;
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-word;
  background: var(--md-code-bg-color);
  color: var(--md-code-fg-color);
  border-top: 1px solid var(--md-default-fg-color--lightest);
}
.tapio-pg__hint { color: var(--md-default-fg-color--light); font-style: italic; }
.tapio-pg__out[data-kind="error"] { color: #d32f2f; }
</style>

<!--
  The playground loader. Pyodide is pulled from jsdelivr on first run only.
  Bump PYODIDE_VERSION to move to a newer interpreter; the matching pydantic
  and pydantic-core come bundled with each Pyodide release, so micropip finds
  a compatible pair without our pinning one.
-->
<script src="https://cdn.jsdelivr.net/pyodide/v0.28.0/full/pyodide.js"></script>
<script>
(function () {
  "use strict";

  var PYODIDE_VERSION = "0.28.0";
  var PYODIDE_INDEX = "https://cdn.jsdelivr.net/pyodide/v" + PYODIDE_VERSION + "/full/";

  // Every asset the page needs sits under dist/, next to this page. Resolving
  // against the current directory keeps the project's /tapio/ path prefix and
  // works the same when the docs are served locally.
  var here = new URL(".", window.location.href);
  var distURL = function (name) { return new URL("dist/" + name, here).href; };

  var el = {
    select: document.getElementById("tapio-pg-example"),
    code: document.getElementById("tapio-pg-code"),
    run: document.getElementById("tapio-pg-run"),
    reset: document.getElementById("tapio-pg-reset"),
    status: document.getElementById("tapio-pg-status"),
    out: document.getElementById("tapio-pg-out")
  };

  var manifest = null;      // {wheel, examples: [{name, label, file}]}
  var sources = {};         // name -> source text, fetched once and cached
  var pyodide = null;       // set after the interpreter loads
  var loadingPython = null; // the in-flight load promise, so Run cannot start two

  function setStatus(text, kind) {
    el.status.textContent = text;
    if (kind) { el.status.setAttribute("data-kind", kind); }
    else { el.status.removeAttribute("data-kind"); }
  }

  // The harness that runs one example. It executes the source inside a real
  // module registered in sys.modules, so pydantic can resolve the string
  // forward references the messages use (ActorRef["Pong"] and the like) the
  // way it does for an imported module. The module name is not "__main__", so
  // an example's `if __name__ == "__main__"` block does not fire and its
  // asyncio.run() is never called inside the loop already running here.
  //
  // Only stdout is captured, which is where the examples print their
  // narration. The framework's own supervision logging goes to the browser
  // console instead, so a deliberately injected failure (an example that
  // shows a restart) does not fill the output pane with a traceback that is
  // working as intended. The failed flag is set only when the example itself
  // raises out of the harness, which is what an edit that breaks the code
  // does, and that traceback is shown.
  var RUNNER = [
    "import sys, io, json, types, asyncio, traceback",
    "async def _tapio_playground_run(src):",
    "    name = 'tapio_playground_program'",
    "    mod = types.ModuleType(name)",
    "    mod.__dict__['__name__'] = name",
    "    sys.modules[name] = mod",
    "    buf = io.StringIO()",
    "    out = sys.stdout",
    "    sys.stdout = buf",
    "    failed = False",
    "    try:",
    "        exec(compile(src, '<playground>', 'exec'), mod.__dict__)",
    "        main = mod.__dict__.get('main')",
    "        if main is not None:",
    "            result = main()",
    "            if asyncio.iscoroutine(result):",
    "                await result",
    "    except BaseException:",
    "        failed = True",
    "        traceback.print_exc(file=buf)",
    "    finally:",
    "        sys.stdout = out",
    "    return json.dumps({'text': buf.getvalue(), 'failed': failed})"
  ].join("\n");

  function fetchText(url) {
    return fetch(url).then(function (r) {
      if (!r.ok) { throw new Error("could not load " + url + " (" + r.status + ")"); }
      return r.text();
    });
  }

  function loadExample(name) {
    if (sources[name]) {
      el.code.value = sources[name];
      return Promise.resolve();
    }
    var entry = manifest.examples.filter(function (e) { return e.name === name; })[0];
    return fetchText(distURL(entry.file)).then(function (text) {
      sources[name] = text;
      el.code.value = text;
    });
  }

  // Load Pyodide and install tapio. Deps for the wheel are installed by name
  // first so micropip takes pydantic and pydantic-core from the Pyodide
  // bundle, then the wheel goes in with deps off so nothing is re-resolved
  // against PyPI.
  function ensurePython() {
    if (pyodide) { return Promise.resolve(); }
    if (loadingPython) { return loadingPython; }
    setStatus("Downloading Python and tapio (first run only, a few megabytes)…");
    loadingPython = loadPyodide({ indexURL: PYODIDE_INDEX }).then(function (py) {
      pyodide = py;
      return py.loadPackage("micropip");
    }).then(function () {
      pyodide.globals.set("_tapio_wheel_url", distURL(manifest.wheel));
      return pyodide.runPythonAsync([
        "import micropip",
        "await micropip.install(['pydantic', 'pydantic-settings'])",
        "await micropip.install(_tapio_wheel_url, deps=False)",
        "import tapio"
      ].join("\n"));
    }).then(function () {
      return pyodide.runPythonAsync(RUNNER);
    }).catch(function (err) {
      loadingPython = null; // let a later Run try again
      throw err;
    });
    return loadingPython;
  }

  function run() {
    el.run.disabled = true;
    el.reset.disabled = true;
    el.out.removeAttribute("data-kind");
    ensurePython().then(function () {
      setStatus("Running…");
      el.out.textContent = "";
      pyodide.globals.set("_tapio_src", el.code.value);
      return pyodide.runPythonAsync("await _tapio_playground_run(_tapio_src)");
    }).then(function (output) {
      var result = JSON.parse(String(output));
      el.out.textContent = result.text || "(the example produced no output)";
      if (result.failed) {
        el.out.setAttribute("data-kind", "error");
        setStatus("The example raised. See the traceback below.", "error");
      } else {
        setStatus("Done. Edit the code and run it again.");
      }
    }).catch(function (err) {
      el.out.textContent = String(err && err.message ? err.message : err);
      el.out.setAttribute("data-kind", "error");
      setStatus("Could not load the interpreter. Check the network and retry.", "error");
    }).then(function () {
      el.run.disabled = false;
      el.reset.disabled = false;
    });
  }

  // A Tab in the editor should indent, not jump to the next control.
  el.code.addEventListener("keydown", function (e) {
    if (e.key !== "Tab") { return; }
    e.preventDefault();
    var start = el.code.selectionStart;
    var end = el.code.selectionEnd;
    el.code.value = el.code.value.slice(0, start) + "    " + el.code.value.slice(end);
    el.code.selectionStart = el.code.selectionEnd = start + 4;
  });

  el.select.addEventListener("change", function () {
    delete sources[el.select.value]; // reload a clean copy when switching
    loadExample(el.select.value);
  });
  el.reset.addEventListener("click", function () {
    delete sources[el.select.value];
    loadExample(el.select.value);
    setStatus("Reset to the original example.");
  });
  el.run.addEventListener("click", run);

  if (typeof loadPyodide === "undefined") {
    setStatus("Could not reach the Pyodide CDN, so the playground cannot run here.", "error");
    return;
  }

  fetchText(distURL("manifest.json")).then(function (text) {
    manifest = JSON.parse(text);
    manifest.examples.forEach(function (e) {
      var opt = document.createElement("option");
      opt.value = e.name;
      opt.textContent = e.label;
      el.select.appendChild(opt);
    });
    return loadExample(manifest.examples[0].name);
  }).then(function () {
    el.run.disabled = false;
    setStatus("Ready. Press Run to start Python and run the example.");
  }).catch(function (err) {
    setStatus(String(err && err.message ? err.message : err), "error");
  });
})();
</script>
