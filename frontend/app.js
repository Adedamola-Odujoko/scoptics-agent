document.addEventListener("DOMContentLoaded", () => {
  // --- 1. GET UI ELEMENTS ---
  const queryInput = document.getElementById("query-input");
  const submitButton = document.getElementById("submit-button");
  const chatContainer = document.getElementById("chat-container");
  const loader = document.getElementById("loader");

  // --- 2. DEFINE API AND STATE ---
  const API_URL = "http://127.0.0.1:8000/agent/query";
  let chatHistory = [];
  let lastData = null;

  // --- 3. CORE QUERY HANDLER ---
  const handleQuery = async () => {
    const query = queryInput.value.trim();
    if (!query) return;

    displayUserMessage(query);
    queryInput.value = "";
    loader.style.display = "block";
    submitButton.disabled = true;

    try {
      const response = await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: query,
          chat_history: chatHistory,
          last_data: lastData,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "An API error occurred.");
      }

      const data = await response.json();
      console.log("AGENT RESPONSE:", data.response);

      chatHistory = data.response.updated_history || [];
      lastData = data.response.data || null;

      displayAgentResponse(data.response);
    } catch (error) {
      console.error("An error occurred:", error);
      displayError(error.message);
    } finally {
      // This FINALLY block is the safety net that GUARANTEES the UI unfreezes.
      loader.style.display = "none";
      submitButton.disabled = false;
      queryInput.focus();
    }
  };

  // --- 4. UI DISPLAY FUNCTIONS ---

  const displayUserMessage = (message) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message user-message";
    messageDiv.textContent = message;
    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const displayError = (errorMessage) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message agent-message";
    const errorP = document.createElement("p");
    errorP.className = "error-message";
    errorP.textContent = `Error: ${errorMessage}`;
    messageDiv.appendChild(errorP);
    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const displayAgentResponse = (response) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message agent-message";

    let conversationalText =
      response.conversational_response || "I have processed your request.";

    // --- NEW: Check for low confidence and append the explanation ---
    if (
      response.confidence_score &&
      response.confidence_score < 70 &&
      response.explanation_for_low_confidence
    ) {
      const explanation = response.explanation_for_low_confidence;

      let noteHtml = `<br><br><div style="border-top: 1px solid #ccc; padding-top: 10px; margin-top: 10px;">`;
      noteHtml += `<b>Note (Confidence: ${response.confidence_score}%)</b><ul style="margin: 5px 0 0 20px; padding: 0;">`;
      if (explanation.reason) {
        noteHtml += `<li><b>Reason:</b> ${explanation.reason}</li>`;
      }
      if (explanation.limitations) {
        noteHtml += `<li><b>Limitations:</b> ${explanation.limitations}</li>`;
      }
      // This is the key part that brings back the suggestion
      if (explanation.suggestion) {
        noteHtml += `<li><b>Suggestion:</b> ${explanation.suggestion}</li>`;
      }
      noteHtml += `</ul></div>`;

      conversationalText += noteHtml;
    }

    const textP = document.createElement("p");
    textP.innerHTML = conversationalText.replace(/\*\*(.*?)\*\*/g, "<b>$1</b>");
    messageDiv.appendChild(textP);

    // Part 2: Try to build and display the visualization
    try {
      const viz = response.visualization;
      if (viz && viz.type !== "none" && viz.payload) {
        const vizContainer = document.createElement("div");
        messageDiv.appendChild(vizContainer);

        switch (viz.type) {
          case "line_chart":
          case "scatter_plot":
            renderChartJS(vizContainer, viz);
            break;
          case "heatmap":
            renderHeatmap(vizContainer, viz);
            break;
          default:
            console.warn(`Unknown visualization type: ${viz.type}`);
        }
      }
    } catch (e) {
      console.error("Could not render visualization:", e);
    }

    // Part 3: Display the collapsible raw data
    if (
      response.data &&
      Array.isArray(response.data) &&
      response.data.length > 0
    ) {
      const dataCard = document.createElement("details");
      const summary = document.createElement("summary");
      summary.textContent = "Show Raw Data";
      dataCard.appendChild(summary);
      const dataPre = document.createElement("pre");
      dataPre.className = "data-card";
      dataPre.textContent = JSON.stringify(response.data, null, 2);
      dataCard.appendChild(dataPre);
      messageDiv.appendChild(dataCard);
    }

    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const scrollToBottom = () => {
    chatContainer.scrollTop = chatContainer.scrollHeight;
  };

  // --- 5. VISUALIZATION RENDERER FUNCTIONS ---

  function renderChartJS(container, viz) {
    const canvas = document.createElement("canvas");
    container.appendChild(canvas);

    // Defensive check for payload data
    if (!viz.payload || !Array.isArray(viz.payload.datasets)) return;

    const finalDatasets = viz.payload.datasets.map((recipe) => {
      // ... (data transformation logic - this part is complex but less likely to crash)
      return {
        label: recipe.label,
        data: recipe.data, // Assumes agent pre-formats data into {x,y} points
        borderColor: recipe.borderColor || "rgba(59, 130, 246, 1)",
        backgroundColor: recipe.borderColor
          ? recipe.borderColor.replace("1)", "0.2)")
          : "rgba(59, 130, 246, 0.2)",
        pointRadius: viz.type === "scatter_plot" ? 5 : 2.5,
        tension: 0.1,
      };
    });

    new Chart(canvas.getContext("2d"), {
      type: viz.type === "scatter_plot" ? "scatter" : "line",
      data: { datasets: finalDatasets },
      options: {
        responsive: true,
        plugins: {
          title: { display: !!viz.options.title, text: viz.options.title },
          legend: { display: finalDatasets.length > 1 },
          tooltip: {
            callbacks: {
              label: (context) =>
                context.raw && context.raw.tooltip
                  ? context.raw.tooltip.split("\\n")
                  : context.formattedValue,
            },
          },
        },
        scales: {
          x: {
            title: {
              display: !!viz.options.xAxisLabel,
              text: viz.options.xAxisLabel,
            },
          },
          y: {
            title: {
              display: !!viz.options.yAxisLabel,
              text: viz.options.yAxisLabel,
            },
          },
        },
      },
    });
  }

  function renderHeatmap(container, viz) {
    if (!viz.payload || !Array.isArray(viz.payload.data)) return;

    const heatmapContainer = document.createElement("div");
    heatmapContainer.className = "heatmap-container";
    container.appendChild(heatmapContainer);

    setTimeout(() => {
      try {
        const heatmapInstance = h337.create({ container: heatmapContainer });
        const width = heatmapContainer.offsetWidth;
        const height = heatmapContainer.offsetHeight;

        const transformedData = viz.payload.data.map((point) => ({
          x: Math.round(((point.x + 52.5) / 105) * width),
          y: Math.round(((point.y + 34) / 68) * height),
          value: point.value,
        }));

        heatmapInstance.setData({
          max: Math.max(...transformedData.map((d) => d.value)),
          data: transformedData,
        });
      } catch (e) {
        console.error("Heatmap rendering failed:", e);
      }
    }, 0);
  }

  // --- 6. ATTACH EVENT LISTENERS ---
  submitButton.addEventListener("click", handleQuery);
  queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault(); // Stop the default form submit action
      handleQuery();
    }
  });
});
