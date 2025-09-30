document.addEventListener("DOMContentLoaded", () => {
  const queryInput = document.getElementById("query-input");
  const submitButton = document.getElementById("submit-button");
  const chatContainer = document.getElementById("chat-container");
  const loader = document.getElementById("loader");

  const API_URL = "http://127.0.0.1:8000/agent/query";

  let chatHistory = [];
  let lastData = null; // Cache for the last data result

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
      console.log("RAW RESPONSE FROM AGENT:", data.response); // For debugging

      chatHistory = data.response.updated_history || [];
      lastData = data.response.data || null;

      displayAgentResponse(data.response);
    } catch (error) {
      console.error("Error fetching data:", error);
      displayError(error.message);
    } finally {
      loader.style.display = "none";
      submitButton.disabled = false;
      queryInput.focus();
    }
  };

  const displayUserMessage = (message) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message user-message";
    messageDiv.textContent = message;
    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const displayAgentResponse = (response) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message agent-message";

    if (response.conversational_response) {
      const textP = document.createElement("p");
      // Use innerHTML to render bold/italic markdown if the agent uses it
      textP.innerHTML = response.conversational_response.replace(
        /\*\*(.*?)\*\*/g,
        "<b>$1</b>"
      );
      messageDiv.appendChild(textP);
    }

    // --- NEW, ROBUST VISUALIZATION LOGIC ---
    // Check if the visualization key exists and its type is not 'none'
    if (response.visualization && response.visualization.type !== "none") {
      const vizContainer = document.createElement("div");
      vizContainer.className = "visualization-container";
      vizContainer.style.marginTop = "1rem";
      vizContainer.style.padding = "1rem";
      vizContainer.style.backgroundColor = "#f8f9fa";
      vizContainer.style.borderRadius = "8px";

      const canvas = document.createElement("canvas");
      const canvasId = `chart-${Date.now()}`; // Unique ID for each chart
      canvas.id = canvasId;

      vizContainer.appendChild(canvas);
      messageDiv.appendChild(vizContainer);

      // Render the chart after the element is added to the DOM
      setTimeout(() => renderChart(canvasId, response.visualization), 0);
    } else if (response.data && response.data.length > 0) {
      // Fallback to showing the raw data card if no visualization is provided
      const cardDiv = document.createElement("div");
      cardDiv.className = "data-card";
      cardDiv.textContent = JSON.stringify(response.data, null, 2);
      messageDiv.appendChild(cardDiv);
    }

    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const renderChart = (canvasId, vizData) => {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    let datasets = [];
    const labels = vizData.data.map((d) =>
      new Date(d.x).toLocaleTimeString([], {
        minute: "2-digit",
        second: "2-digit",
      })
    );

    // Check if the first data point's 'y' is an object (for multi-line charts)
    if (typeof vizData.data[0].y === "object" && vizData.data[0].y !== null) {
      const keys = Object.keys(vizData.data[0].y);
      const colors = ["#3b82f6", "#ef4444", "#10b981", "#f97316"];
      keys.forEach((key, index) => {
        datasets.push({
          label: key.replace(/_/g, " ").replace("avg", "Avg."), // Nicer labels
          data: vizData.data.map((d) => d.y[key]),
          borderColor: colors[index % colors.length],
          tension: 0.1,
          fill: false,
        });
      });
    } else {
      // Handle single-line charts
      datasets.push({
        label: vizData.options.yAxisLabel || "Value",
        data: vizData.data.map((d) => d.y),
        borderColor: "#3b82f6",
        tension: 0.1,
        fill: false,
      });
    }

    new Chart(ctx, {
      type: vizData.type.split("_")[0], // 'line_chart' -> 'line'
      data: {
        labels: labels,
        datasets: datasets,
      },
      options: {
        responsive: true,
        plugins: {
          title: {
            display: true,
            text: vizData.options.title,
            font: { size: 16 },
          },
          legend: {
            position: "top",
          },
        },
        scales: {
          x: {
            title: {
              display: true,
              text: vizData.options.xAxisLabel,
            },
          },
          y: {
            title: {
              display: true,
              text: vizData.options.yAxisLabel,
            },
          },
        },
      },
    });
  };

  const displayError = (message) => {
    const errorDiv = document.createElement("div");
    errorDiv.className = "chat-message agent-message";
    errorDiv.innerHTML = `<p class="error-message">Error: ${message}</p>`;
    chatContainer.appendChild(errorDiv);
    scrollToBottom();
  };

  const scrollToBottom = () => {
    chatContainer.scrollTop = chatContainer.scrollHeight;
  };

  submitButton.addEventListener("click", handleQuery);
  queryInput.addEventListener("keyup", (event) => {
    if (event.key === "Enter") {
      handleQuery();
    }
  });
});
