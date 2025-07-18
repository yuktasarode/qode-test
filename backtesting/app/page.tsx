"use client";

import { useState } from "react";
import axios from 'axios';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

type BacktestResult = {
  equity_curve: { date: string; value: number }[];
  drawdown_curve: { date: string; drawdown: number }[];
  metrics: {
    cagr: number;
    sharpe: number;
    max_drawdown: number;
  };
};


export default function Home() {
  const [pingResponse, setPingResponse] = useState("");
  const [echoResponse, setEchoResponse] = useState("");

  const handlePing = async () => {
  try {
    const res = await axios.get("http://localhost:8000/ping");
    setPingResponse(JSON.stringify(res.data));
  } catch (err) {
    setPingResponse("Error contacting backend");
  }
};

  const handleEcho = async () => {
  try {
    const res = await axios.post("http://localhost:8000/echo", {
      message: "Yukta",
    });
    setEchoResponse(JSON.stringify(res.data));
  } catch (err) {
    setEchoResponse("Error contacting backend");
  }
};


  // Real Code

  const [config, setConfig] = useState({
    initial_capital: 100000,
    start_date: '2023-01-01',
    end_date: '2023-03-03',
    rebalance_frequency: 'monthly',
    position_sizing: 'equal',
    portfolio_size: 5,
    market_cap_min: 100000,
    market_cap_max: 2000000,
    roce: 5,
    pat: 0,
    ranking: 'roe:desc',
    compranking: 'yes'
  });
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [loading, setLoading] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
  const { name, value } = e.target;
  const numericFields = [
    'portfolio_size', 'initial_capital',
    'market_cap_min', 'market_cap_max',
    'roce', 'pat'
  ];
  
  setConfig({
    ...config,
    [name]: numericFields.includes(name) ? Number(value) : value,
  });
};

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setLoading(true);
    try {
      console.log(config)
      const response = await axios.post('http://localhost:8000/run-backtest', config);
      setResult(response.data);
      console.log(response.data)
    } catch (err) {
      console.error(err);
      alert("Error running backtest");
    }
    setLoading(false);
  };

  const handleExport = async () => {
    try {
      const response = await axios.get('http://localhost:8000/export-backtest', {
        responseType: 'blob',
      });

      const blob = new Blob([response.data], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'backtest_results.csv';
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (err) {
      console.error(err);
      alert("Error exporting results");
    }
  };


  // Real Code



  return (
    <div className="font-sans grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        <button
          onClick={handlePing}
          className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600"
        >
          Test /ping
        </button>
        <p className="text-sm text-white-800">{pingResponse}</p>

        <button
          onClick={handleEcho}
          className="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600"
        >
          Test /echo
        </button>
        <p className="text-sm text-white-800">{echoResponse}</p>

        {/* Real code */}

        <div>
          <div className="p-8 max-w-4xl mx-auto">
          <h1 className="text-2xl font-bold mb-4">Equity Backtesting Platform</h1>

          
          <form onSubmit={handleSubmit} className="space-y-4">
            <h1> Set Parameters</h1>
            <div className="grid grid-cols-2 gap-4">
              <p>Initial Capital</p>
              <input type="text" name="initial_capital" placeholder="Initial investment" value={config.initial_capital} onChange={handleChange} className="border p-2" />
              <p>Start Date</p>
              <input type="date" name="start_date" value={config.start_date} onChange={handleChange} className="border p-2" />
              <p>End Date</p>
              <input type="date" name="end_date" value={config.end_date} onChange={handleChange} className="border p-2" />
              <p>Rebalance Frequencey</p>
              <select name="rebalance_frequency" value={config.rebalance_frequency} onChange={handleChange} className="border p-2 bg-gray-800">
                <option value="monthly">Monthly</option>
                <option value="quarterly">Quarterly</option>
                <option value="yearly">Yearly</option>
              </select>
              <p>Portfolio Size</p>
              <input type="text" name="portfolio_size" placeholder="Portfolio Size" value={config.portfolio_size} onChange={handleChange} className="border p-2" />
              
              <p>Position Sizing</p>
              <select name="position_sizing" value={config.position_sizing} onChange={handleChange} className="border p-2 bg-gray-800">
                <option value="equal">Equal-weighted</option>
                <option value="market_cap">Market Cap-weighted</option>
                <option value="roce">ROCE-weighted</option>
              </select>

             <h2>--</h2>
             <h2>--</h2>

              <h2>Filtering System</h2>
             

              <p>Market Cap Range</p>
              <input type="text" name="market_cap_min" placeholder="Market Cap Min" value={config.market_cap_min} onChange={handleChange} className="border p-2" />
              <input type="text" name="market_cap_max" placeholder="Market Cap Max" value={config.market_cap_max} onChange={handleChange} className="border p-2" />
              
              <p>ROCE</p>
              <input type="text" name="roce" placeholder="ROCE > X%" value={config.roce} onChange={handleChange} className="border p-2" />
              
              <p>PAT</p>
              <input type="text" name="pat" placeholder="PAT > X" value={config.pat} onChange={handleChange} className="border p-2" />

              <h2>--</h2>
              <h2>--</h2>

              <h2>Ranking System</h2>
              <select name="ranking" value={config.ranking} onChange={handleChange} className="border p-2 bg-gray-800">
                <option value="roe:desc">Roe desc</option>
                <option value="roe:asc">Roe asc</option>
                <option value="pe:desc">pe desc</option>
                <option value="pe:asc">pe asc</option>
              </select>

              <p>Use composite ranking</p>
              <select name="compranking" value={config.compranking} onChange={handleChange} className="border p-2 bg-gray-800">
                <option value="yes">yes</option>
                <option value="no">no</option>
              </select>
            </div>

            <div className="flex gap-4">
              <button type="submit" className="bg-blue-600 text-white px-4 py-2 rounded">
                {loading ? 'Running...' : 'Run Backtest'}
              </button>
              {result && (
                <button type="button" onClick={handleExport} className="bg-green-600 text-white px-4 py-2 rounded">
                  Export CSV
                </button>
              )}
            </div>
          </form>

          {result && (
            <div className="mt-8">
              <h2 className="text-xl font-semibold mb-2">Equity Curve</h2>
              <Line
                data={{
                  labels: result.equity_curve.map(d => d.date),
                  datasets: [
                    {
                      label: 'Portfolio Value',
                      data: result.equity_curve.map(d => d.value),
                      borderColor: 'rgb(75, 192, 192)',
                      tension: 0.1
                    }
                  ]
                }}
              />
              <h2 className="text-xl font-semibold mt-8 mb-2">Drawdown Curve</h2>
              <Line
                data={{
                  labels: result.drawdown_curve.map(d => d.date),
                  datasets: [
                    {
                      label: 'Drawdown',
                      data: result.drawdown_curve.map(d => d.drawdown),
                      borderColor: 'rgb(255, 99, 132)',
                      tension: 0.1
                    }
                  ]
                }}
              />
              <div className="mt-4">
                <p><strong>CAGR:</strong> {result.metrics.cagr}%</p>
                <p><strong>Sharpe:</strong> {result.metrics.sharpe}</p>
                <p><strong>Max Drawdown:</strong> {result.metrics.max_drawdown}%</p>
              </div>
            </div>
          )}
        </div>

        </div>
        
      </main>
    </div>

    
  );
}
