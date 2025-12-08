'use client'
import { Pie, Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  BarElement,
  Title
} from 'chart.js';
import migrationReport from '../../logs/migration_report_20251128_123459.json'; // adjust path if needed

ChartJS.register(ArcElement, Tooltip, Legend, CategoryScale, LinearScale, BarElement, Title);

export default function Home() {
  // Load the data directly from the imported JSON
  const reportData = migrationReport;
  const locationData = reportData.sheets.Location;

  const statusData = {
    labels: ['Success', 'Upload Errors', 'Validation Errors', 'Mapping Errors'],
    datasets: [
      {
        label: 'Row Status',
        data: [
          locationData.success_count,
          locationData.upload_errors,
          locationData.validation_errors,
          locationData.mapping_errors
        ],
        backgroundColor: ['#4CAF50', '#F44336', '#FF9800', '#2196F3']
      }
    ]
  };

  const successRateData = {
    labels: ['Success Rate (%)'],
    datasets: [
      {
        label: 'Success Rate',
        data: [locationData.success_rate],
        backgroundColor: ['#4CAF50']
      }
    ]
  };

  const pieOptions = { responsive: true, plugins: { title: { display: true, text: 'Data Cleanliness Overview' } } };
  const barOptions = {
    responsive: true,
    plugins: { title: { display: true, text: 'Migration Success Rate' } },
    scales: { y: { min: 0, max: 100, title: { display: true, text: 'Percentage' } } }
  };

  return (
    <div style={{ padding: 20 }}>
      <h1>Data Migration Dashboard</h1>

      <div style={{ width: 600, marginBottom: 40 }}>
        <Pie data={statusData} options={pieOptions} />
      </div>

      <div style={{ width: 600, marginBottom: 40 }}>
        <Bar data={successRateData} options={barOptions} />
      </div>
    </div>
  );
}
