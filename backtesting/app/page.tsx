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
  run_id:string;
  equity_curve: { date: string; value: number }[];
  drawdown_curve: { date: string; drawdown: number }[];
  metrics: {
    cagr: number;
    sharpe: number;
    max_drawdown: number;
  };
};


export default function Home() {
  const [runId, setRunId] = useState<string | null>(null);
  const [niftyData, setNiftyData] = useState<{ date: string; value: number }[] | null>(null);


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
    ranking: ['roe:desc'],
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
      const response = await axios.post('http://localhost:8000/run-backtest', {
        ...config,
        ranking: config.ranking.join(",") 
      });
      setResult(response.data);
      setRunId(response.data.run_id);
      console.log(response.data)
    } catch (err) {
      console.error(err);
      alert("Error running backtest");
    }
    setLoading(false);
  };

  const handleNifty=async()=>{
    try {
    const response = await axios.post("http://localhost:8000/compute-nifty", {
      ...config,
      ranking: config.ranking.join(","),
    });
    setNiftyData(response.data);
  } catch (err) {
    console.error(err);
    alert("Error fetching Nifty50 data");
  }

  }

  const handleRankingChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
  const selectedOptions = Array.from(e.target.selectedOptions).map(option => option.value);
  setConfig(prev => ({
    ...prev,
    ranking: selectedOptions
  }));
};

  const handleExport = async () => {

    if (!runId) {
    alert("Please run the backtest first.");
    return;
  }

    try {
      const response = await axios.get(`http://localhost:8000/export-backtest`, {
      params: { run_id: runId }, // send run_id as query param
      responseType: 'blob',
    });

      const blob = new Blob([response.data], { type: 'application/zip' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${runId}_backtest_export.zip`;
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
    <div className="font-sans grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen pt-4 pb-20 px-8 sm:pt-10 sm:px-20">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        
          <h1 className="text-2xl font-bold mb-4 mx-auto">Equity Backtesting Platform</h1>

          <form onSubmit={handleSubmit} className="space-y-4">

            <div className="flex flex-col sm:flex-row gap-4 w-full">

              <div className="w-full border border-white rounded-lg bg-gray-800 p-4 text-white">
                <p className="text-base mb-8 font-semibold text-center">Parameters</p>
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
                </div>
              </div>

              <div className="w-full border border-white rounded-lg bg-gray-800 p-4 text-white">
                <p className="text-base mb-8 font-semibold text-center">Filters</p>
                <div className="grid grid-cols-2 gap-4">
                <p>Market Cap Range</p>
                <p></p>
                <p>Min</p><input type="text" name="market_cap_min" placeholder="Market Cap Min" value={config.market_cap_min} onChange={handleChange} className="border p-2" />
                <p>Max</p><input type="text" name="market_cap_max" placeholder="Market Cap Max" value={config.market_cap_max} onChange={handleChange} className="border p-2" />
                <p>ROCE</p><input type="text" name="roce" placeholder="ROCE > X%" value={config.roce} onChange={handleChange} className="border p-2" />
                <p>PAT</p><input type="text" name="pat" placeholder="PAT > X" value={config.pat} onChange={handleChange} className="border p-2" />
                </div>
              </div>

              <div className="w-full border border-white rounded-lg bg-gray-800 p-4 text-white">
                <p className="text-base mb-8 font-semibold text-center">Ranking</p>
                <div className="grid grid-cols-2 gap-4">
                  <p>Criteria</p>
                  <select name="ranking"  multiple value={Array.isArray(config.ranking) ? config.ranking : [config.ranking]} onChange={handleRankingChange} className="border p-2 bg-gray-800">
                    <option value="roe:desc">Roe desc</option>
                    <option value="roe:asc">Roe asc</option>
                    <option value="pe:desc">pe desc</option>
                    <option value="pe:asc">pe asc</option>
                  </select>
                  <p>Composite ranking</p>
                  <select name="compranking" value={config.compranking} onChange={handleChange} className="border p-2 bg-gray-800">
                    <option value="yes">yes</option>
                    <option value="no">no</option>
                  </select>
                </div>
              </div>

            </div>

            <div className="flex gap-4">
              <button type="submit" className="bg-blue-600 text-white px-4 py-2 rounded">
                {loading ? 'Running...' : 'Run Backtest'}
              </button>
              {result && (
                <div className="flex gap-4">
                <button type="button" onClick={handleExport} className="bg-green-600 text-white px-4 py-2 rounded">
                  Export CSV
                </button>

                <button type="button" onClick={handleNifty} className="bg-green-600 text-white px-4 py-2 rounded">
                  Compare Nifty50
                </button>
                </div>
              )}
            </div>
          </form>

          {result && (
            <div className="mt-8 w-full">
              <div className="flex flex-col sm:flex-row gap-8">
                <div className="flex-1">
                  <h2 className="text-xl font-semibold mb-2">Equity Curve</h2>
                  <div className="w-full h-[400px]">
                  <Line
                    data={{
                      labels: result.equity_curve.map(d => d.date),
                      datasets: [
                        {
                          label: 'Portfolio Value',
                          data: result.equity_curve.map(d => d.value),
                          borderColor: 'rgb(75, 192, 192)',
                          tension: 0.1
                        },
                        ...(niftyData
                          ? [{
                              label: 'Nifty50',
                              data: niftyData.map(d => d.value),
                              borderColor: 'rgb(255, 206, 86)',
                              borderDash: [5, 5],
                              tension: 0.1
                            }]
                          : [])
                      ]
                    }}
                    options={{ maintainAspectRatio: false, responsive: true }}
                  />
                  </div>
                </div>

                <div className="flex-1">
                  <h2 className="text-xl font-semibold mt-8 mb-2">Drawdown Curve</h2>
                  <div className="w-full h-[400px]">
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
                    options={{ maintainAspectRatio: false, responsive: true }}
                  />
                  </div>
                </div>

              </div>


              <div className="mt-4">
                <p><strong>CAGR:</strong> {result.metrics.cagr}%</p>
                <p><strong>Sharpe:</strong> {result.metrics.sharpe}</p>
                <p><strong>Max Drawdown:</strong> {result.metrics.max_drawdown}%</p>
              </div>
            </div>
          )}
       
        
      </main>
    </div>

    
  );
}
